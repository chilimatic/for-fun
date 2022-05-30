#### Possible concepts

1. straight forward semi object oriented single threaded linear processing 
2. state machines utilizing the type system for state transitioning
4. functional approach utilizing the compilers understanding of monads to auto optimize memory usage

##### straight forward approach
 - 2 BTrees for the storage and lookup for client accounts and the transaction
   - 1 tree has the client id as key and contains the accounts
   - 1 tree has the transaction id and the amount inside
 - to avoid float problems -> transform to u64 
   - size decision -> maybe ridiculous amounts are traded
   - unsigned -> we're not allowed to have negative values we are using "double accounting" 
     this means there are no negative values. 
 - procedural approach
   - for processing we parse, transform and pass the values to our business logic
   - Our ClientAccount struct is rather simple/stupid and only has the basic checks 
     if the amount and action would work
   - if conditions and match are used to ensure the logic works

Downside is that we are not using the type system to ensure certain states are not possible, 
but rather normal checks.

##### State machine utilizing From

in this case we just have 1 btree (why? because I decided it, and it does work, I just did not want to log the transactions in the client_account in approach 1 but rather have heap based objects) 
since our state transition of an open dispute can just push the transaction into a vector bound to the client_account struct

we could also use a fixed / decimal data type and hide the conversion to an int value 

```rust 
enum State {
    Open,
    InDispute(Option<Vec<(i32, u64)>>),
    Locked
}

struct ClientAccount<S> {
  id: u16,
  available: u64,
  held: u64,
  state: S
}

impl From<ClientAccount<Open>> for ClientAccount<InDispute> {
    fn from(val: ClientAccount<Open>, event: &AccountEvent) -> ClientAccount<InDispute> {
        ClientAccount {
           id: val.id,
           available: val.available - event.amount,
           held: val.held + event.amount,
           state: State::InDispute(vec![(event.transaction_id, event.amount)])
        }
    }
}
```
this approach has some nice type safety for more details https://hoverbear.org/blog/rust-state-machine-pattern/

ofc we could also use enum wrappings for the account and just make a double match, but again 
this would work as runtime type safety within the sum types but not in compile time that we do something stupid.


#### fancy but useless for our purpose
4. actor model with message passing
5. thread pooling 
6. futures 

we cannot really utilize the power of parallelization since we are going for a monotonic
stream approach because we cannot guarantee the order of processing between the batches
So we could have thread A having the deposit with the transaction and thread B executes first not 
finding the transaction id in the lookup so even if it's valid we could fail. 

this is about money, consistency and correctness is key! 
And just adding the overhead of sys calls, so we can say it's nonblocking ... rather use this program 
as a highlevel map reduce.  

##### possible optimizations
make a first pass and order by client allows batch processing, but do we have monotonic transaction ids?
this would allow to actually utilize parallelization in sensible batch process, but the question is 
if this actually makes sense at all since the sorting is O(n) or even with quicksort, workstealing and 
other optimizations this is "a cursor based" approach, so we stream from A to B and allocate only what is needed
keeping the events as a CSV instead of having it in memory all the time.

We're missing metrics of IO and memory consumption etc so pre optimizing is fun but maybe another time 
and according to the indicated dataset of size, we rather should go for the stream approach.


### please look into the code for more information

This was fun I spent roughly 8 hours on it in total. understanding the problem creating test sets, writing diagrams 
and those things.

I don't really care if I am taken or not, this alone was fun enough for a while also it was nice not having
to work in a restricted TEE so std is available and no FFI for C++, very relaxing

