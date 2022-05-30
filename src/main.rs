extern crate csv;
#[macro_use]
extern crate log;
extern crate serde;

use std::collections::BTreeMap;
use std::env;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use log::debug;

use serde::{Deserialize, Serialize};

/// Certain assumptions: Floatings point numbers are tricky because 0.9 = 1 as we know from math and this attribute
/// leads to our famous need for radix and other things because memory size and representation is tricky
/// to avoid this we can either go for a fixed number or just remain within the realm of natural integers
/// and only use it for representation purposes
///
/// since our transaction is defined with 4 decimal places we just multiply it by 10000 and this already allows us
/// to ignore this hazard and the transformations just have to occur on the beginning and the end
///
/// we are not allowed to go into the negative number range so we are in ℕ and within ℕ we're in ℵ0
/// fancy way of saying "more than we can count" in math.
///
/// u32 should have a range of 4 294 967 295 so just for the sake of it I will use u64
///
/// core flow idea
/// ---------    --------------    ------------------    ---------------
/// | start | -> | stream csv | -> | process events | -> | display csv |
/// ---------    --------------    ------------------    ---------------
///
/// we have 5 Actions
///  - withdraw
///  - deposit
///  - dispute
///  - resolve
///  - chargeback
///
/// we have a transaction stream that is applied to customer accounts
/// we can be out of order (sequential consistency) so a form of eventual consistency
/// if we do this in a concurrent stream form we have the obvious order problem to avoid this problem
/// we will also log the last transaction within the account so we know that 6 cannot be applied before 2
/// this still does not solve the possible dispute problem.
/// I am not sure how the definition of a dispute is. Can it be 1 week later? 1h? 10sec?
///
/// now we have the resource problem to address, how can we store and apply things,
/// if we just do resource streaming, this would solve some of the memory problems.
///
/// Since none of these problems are addressed in the specification I will take the simplest and safest
/// approach. which means no concurrency (order problems) so this will basically a simple stream with 2
/// btrees as storage engine (client_id, ClientAccount) and (transaction_id, AccountEvent)
///
/// this should hopefully allows us quick lookups.
///
///
/// we assume for argument sake 1 million records in the csv
/// so we have size wise per record 24 bytes -> as seen in the test
///
/// u16 + i32 + u64 + enum (u8) https://fasterthanli.me/articles/peeking-inside-a-rust-enum
/// which roughly would be 22.888183594 MB if we keep it in memory (ofc I ignore the allocation of the BTree which basically will
/// now we only need to store the actual ones with money in which with luck means an even smaller footprint
/// since we're storing it in an BTreeMap this allows us to have theoretical O(1) lookup time to discard invalid transactions / out of order transactions
///
/// so lets assume ~ 50MB for 1 Million entries if we only have deposits
///
/// the problem in general is that in theory if these are web streams
/// that we cannot assume the right order so probably what we will need is a retry consolidation concept.
/// but the amount of different approach is just to much
///
/// This is concept 1: straight forward
/// Concept 2 would be using a state-machine using "from"
/// would be more fun but we wanted maintainability as a focus so i kept a rather straight forward approach
/// Concept 3 would be using a functional approach where we let the compiler realize the monads and optimize the resources
/// Concept 4 would be an actor model with message passing and shared resource
///
/// the advantage of async here is not really given we still need to remain linear in processing unless
/// we go from O(n) to O(k + n) so we basically just dispatch things to the kernel creating overhead to appear fancy.
///
/// the specification said we can discard non existing transactions. personally I would add a retry loop after finishing processing
/// to verify no open dispute is there but I already can think of a lot of things that would be needed to be specified I am missing
///
/// also ofc I could've done simple line per line streams or pass by ref things
///   

const FIXED_POINT_SHIFT: f32 = 10000.0;

pub struct AccountProcessing {
    pub accounts: BTreeMap<u16, ClientAccount>,
    pub transaction_amount: BTreeMap<i32, u64>,
}

impl AccountProcessing {
    pub fn run(&mut self, path_to_csv: String) {
        let path = Path::new(&path_to_csv);
        if !path.exists() {
            println!("file does not exist");
            return;
        }

        let file = File::open(path_to_csv).expect("a valid path should be given");
        let buf_reader = BufReader::new(file);
        let mut rdr = csv::Reader::from_reader(buf_reader);

        let _ = rdr
            .deserialize()
            .map(|result: Result<CsvRecord, _>| {
                if let Ok(record) = result {
                    let event = AccountEvent::from(record);
                    if self.dispute_action_with_invalid_transaction(&event) {
                        debug!("no transaction exists in lookup for: {}", &event);
                        return;
                    }

                    self.process_event(&event);

                    // we can only dispute what we have so only things that exist should be able to
                    if !Self::event_needs_transaction_lookup(event.action_type) {
                        debug!("transaction added: {}", &event.transaction_id);
                        self.transaction_amount
                            .insert(event.transaction_id, event.amount.unwrap_or(0));
                    }
                }
            })
            .collect::<()>();

        self.display();
    }

    pub fn process_event(&mut self, event: &AccountEvent) {
        if !self.accounts.contains_key(&event.client_id) {
            let new_client = ClientAccount::new(event.client_id, 0);
            // this can be solved way more beautiful
            debug!("client created with id: {}", &event.client_id);
            self.accounts.insert(new_client.id, new_client);
        }

        let client_account = self.accounts.get_mut(&event.client_id).unwrap();

        // we create a new event for our dispute cases because they don't have an active amount
        if Self::event_needs_transaction_lookup(event.action_type) {
            let transaction = self.transaction_amount.get(&event.transaction_id);
            // should actually be checked before but for sanity reasons
            if transaction.is_none() {
                debug!("non existing transaction for: {}", &event);
                return;
            }

            let amount = *transaction.unwrap();
            let new_event = AccountEvent {
                transaction_id: event.transaction_id,
                action_type: event.action_type,
                client_id: event.client_id,
                amount: Some(amount),
            };

            debug!("new event was created for a dispute event: {}", &new_event);
            Self::apply(client_account, &new_event);

            return;
        }

        debug!("normal event consumed: {}", &event);
        Self::apply(client_account, event);
    }

    pub fn apply(client_account: &mut ClientAccount, event: &AccountEvent) {
        match event.action_type {
            AccountActions::Withdrawal => client_account.withdraw(event.amount.unwrap_or(0)),
            AccountActions::Deposit => client_account.deposit(event.amount.unwrap_or(0)),
            AccountActions::Dispute => client_account.dispute(event.amount.unwrap_or(0)),
            AccountActions::ChargeBack => client_account.charge_back(event.amount.unwrap_or(0)),
            AccountActions::Resolve => client_account.resolve(event.amount.unwrap_or(0)),
        }
    }

    pub fn display(&self) {
        println!("client,available,held,total,locked");
        for client_account in self.accounts.values() {
            println!(
                "{}", // we format with 4 zeros after the dot
                client_account
            );
        }
    }

    /// primarily a semantic extraction. do we really need to inline it? probably not.
    /// but well this as good as any reason https://www.youtube.com/watch?v=QayoudZnjF8 ;)
    #[inline]
    pub fn dispute_action_with_invalid_transaction(
        &mut self,
        account_event: &AccountEvent,
    ) -> bool {
        Self::event_needs_transaction_lookup(account_event.action_type)
            && !self
                .transaction_amount
                .contains_key(&account_event.transaction_id)
    }

    pub fn event_needs_transaction_lookup(account_action: AccountActions) -> bool {
        account_action == AccountActions::ChargeBack
            || account_action == AccountActions::Resolve
            || account_action == AccountActions::Dispute
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AccountActions {
    // only positive if the active available is bigger or equal the withdraw
    Withdrawal,
    // always possible but can be disputed
    Deposit,
    // needs a valid transaction id
    Dispute,
    // if the dispute is resolved there is a chargeback
    ChargeBack,
    // resolve means that the amount of the transaction is either available for held or not
    Resolve,
}

impl Display for AccountActions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AccountActions::Withdrawal => "withdrawal",
            AccountActions::Deposit => "deposit",
            AccountActions::Dispute => "dispute",
            AccountActions::ChargeBack => "chargeback",
            AccountActions::Resolve => "resolve",
        };

        write!(f, "{}", name)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct AccountEvent {
    pub transaction_id: i32,
    pub action_type: AccountActions,
    pub client_id: u16,
    pub amount: Option<u64>,
}

impl Display for AccountEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut amount = String::new();
        if self.amount.is_some() {
            amount = self.amount.unwrap().to_string();
        }

        write!(
            f,
            "{},{},{},{}",
            self.action_type, self.client_id, self.transaction_id, amount
        )
    }
}

impl From<CsvRecord> for AccountEvent {
    fn from(r: CsvRecord) -> Self {
        let amount = r.amount;
        let mut new_amount: Option<u64> = None;
        if let Some(amount) = amount {
            new_amount = Some((amount * FIXED_POINT_SHIFT) as u64)
        }

        AccountEvent {
            transaction_id: r.tx,
            client_id: r.client,
            action_type: r.r#type,
            amount: new_amount,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CsvRecord {
    pub r#type: AccountActions,
    pub client: u16,
    pub tx: i32,
    pub amount: Option<f32>,
}

#[derive(Debug, Copy, Clone, Serialize)]
pub struct ClientAccount {
    // the id is also the lookup in the btree
    pub id: u16,
    // amount of money available for the client
    pub available: u64,
    // amount of money that is held till the dispute is settled
    pub held: u64,
    // possible fraud after chargeback
    pub locked: bool,
}

impl ClientAccount {
    pub fn new(id: u16, deposit: u64) -> Self {
        ClientAccount {
            id,
            available: deposit,
            held: 0,
            locked: false,
        }
    }

    pub fn withdraw(&mut self, amount: u64) {
        if self.locked {
            info!(
                "cannot withdraw: {} from {} client_id {} is locked",
                amount, self.available, self.id
            );
            // locked accounts cannot withdraw
            return;
        }

        // we only check for available since these are the accessible funds even if there is theoretically more that is held
        if amount > self.available {
            info!(
                "client: {}, cannot withdraw: {} from {}",
                self.id, amount, self.available
            );
            return;
        }

        self.available -= amount;
    }

    // we always can let the possible disputes increase
    // so no lock check needed
    pub fn dispute(&mut self, amount: u64) {
        if amount > self.available {
            info!(
                "cannot dispute: {} - is more then the client possesses",
                amount
            );
            return;
        }

        self.available -= amount;
        self.held += amount;
    }

    pub fn deposit(&mut self, amount: u64) {
        if self.locked {
            info!("client_id: {} cannot deposit: {} ", self.id, amount);
            return;
        }

        self.available += amount;
    }

    pub fn charge_back(&mut self, amount: u64) {
        // we can only give back what is there and within the disputed transaction
        if self.held == 0 || self.held < amount {
            info!(
                "client_id: {} cannot charge_back: {} it is more then the client possesses",
                self.id, amount
            );
            return;
        }

        self.held -= amount;
        self.locked = true;
    }

    pub fn resolve(&mut self, amount: u64) {
        if self.held == 0 || self.held < amount {
            info!(
                "client_id: {} cannot resolve: {} it is more then the client holds has to be an error",
                self.id, amount
            );
            return;
        }

        self.held -= amount;
        self.available += amount;
        self.locked = false
    }
}

impl Display for ClientAccount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{:.4},{:.4},{:.4},{}", // we format with 4 zeros after the dot
            self.id,
            (self.available as f32 / FIXED_POINT_SHIFT),
            (self.held as f32 / FIXED_POINT_SHIFT),
            (self.available + self.held) as f32 / FIXED_POINT_SHIFT,
            self.locked
        )
    }
}

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("needs the path of the csv as CLI parameter");
        return;
    }

    let mut app = AccountProcessing {
        accounts: Default::default(),
        transaction_amount: Default::default(),
    };
    let path = args.get(1).expect("a path has to be given");

    app.run(path.to_string());
}

#[cfg(test)]
mod test {
    use crate::{AccountEvent, AccountProcessing, ClientAccount};
    use std::mem;
    #[test]
    fn builder_pattern() {
        let id = 1414;
        let amount = 1414141;

        let client_account = ClientAccount::new(id, amount);
        assert_eq!(
            client_account.id, id,
            "id {} should be {}",
            client_account.id, id
        );
        assert_eq!(
            client_account.held, 0,
            "held {} should be {}",
            client_account.held, amount
        );
        assert_eq!(
            client_account.available, 1414141,
            "available {} should be {}",
            client_account.available, amount
        );
        assert_eq!(
            client_account.locked, false,
            "id {} should be {}",
            client_account.id, id
        );
    }

    #[test]
    fn memory_layout_event() {
        assert_eq!(24, mem::size_of::<AccountEvent>());
    }

    #[test]
    fn memory_layout_client() {
        assert_eq!(24, mem::size_of::<ClientAccount>());
    }

    #[test]
    fn memory_layout_processing() {
        assert_eq!(48, mem::size_of::<AccountProcessing>());
    }

    #[test]
    fn deposit_in_active_client_account() {
        let mut client_account = ClientAccount::new(14, 0);
        client_account.deposit(20);

        assert_eq!(0, client_account.held);
        assert_eq!(20, client_account.available);
    }

    #[test]
    fn deposit_with_dispute_client_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.dispute(20);
        client_account.deposit(20);

        assert_eq!(client_account.available, 20, "it should be 20 available");
        assert_eq!(client_account.held, 20, "it should be 40 held");
    }

    #[test]
    fn withdraw_from_client_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.withdraw(20);

        assert_eq!(client_account.available, 0, "it should be 0 available");
        assert_eq!(client_account.held, 0, "it should be 0 held");
    }

    #[test]
    fn withdraw_to_much_from_client_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.withdraw(40);

        assert_eq!(client_account.available, 20, "it should be 20 available");
        assert_eq!(client_account.held, 0, "it should be 20 held");
    }

    #[test]
    fn withdraw_from_disputed_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.dispute(10);
        client_account.withdraw(10);

        assert_eq!(client_account.available, 0, "it should be 0 available");
        assert_eq!(client_account.held, 10, "it should be 10 held");
    }

    #[test]
    fn withdraw_to_much_from_disputed_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.dispute(10);
        client_account.withdraw(20);

        assert_eq!(client_account.available, 10, "it should be 10 available");
        assert_eq!(client_account.held, 10, "it should be 20 held");
    }

    #[test]
    fn lock_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.dispute(10);
        assert_eq!(client_account.available, 10, "it should be 10 available");
        assert_eq!(client_account.held, 10, "it should be 10 held");

        client_account.charge_back(10);

        assert_eq!(client_account.available, 10, "it should be 10 available");
        assert_eq!(client_account.held, 0, "it should be 0 held");
        assert_eq!(client_account.locked, true, "it should be locked");
    }

    #[test]
    fn resolve_dispute_account() {
        let mut client_account = ClientAccount::new(14, 20);
        client_account.dispute(10);
        client_account.resolve(10);

        assert_eq!(client_account.available, 20, "it should be 10 available");
        assert_eq!(client_account.held, 0, "it should be 10 held");
        assert_eq!(client_account.locked, false, "it should not be locked");
    }
}
