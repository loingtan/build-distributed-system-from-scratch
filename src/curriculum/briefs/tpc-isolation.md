Your two-phase commit is correct when transfers arrive one at a time. Real clients do not wait their turn.

Suppose account `n1.a` holds 100 and two transfers each want 60 of it. Both prepares arrive, both see 100, both vote yes, both commit: the account ends at **-20**. Each check was right when it ran; the second was simply checking money already promised to the first.

## What you build

The same `transfer` and `status` messages as before, with one more guarantee: **no balance ever goes negative**, however many transfers overlap. The tests also read balances while transfers are running.

A yes vote is a promise that the moves can still be applied when commit arrives. Keep it by holding on to what was promised, from the vote until the outcome:

- **Lock** each account a prepared transaction touches; a `prepare` that needs a locked account votes no. Or
- **Reserve** the amount: count money promised to prepared transactions as already gone when checking the next one.

Either way, release it on commit **and** on abort.

## Refuse or wait?

A participant may wait for a lock instead of refusing, but two transactions that each hold one lock and wait for the other's will wait forever. Refusing at once (called *no-wait*) avoids deadlock completely; the client can retry. Whatever you choose, every transfer still gets an answer within one second.

## What counts as correct

Transfers that overlap with others may be refused. But:

- when several transfers race for one account and any one of them fits, **at least one** must commit;
- once nothing else is in flight, a transfer the accounts can afford **must** commit. A lock that is never released shows up here.

## The tests check

- no balance below zero, at any time
- everything from the previous task still holds
- one racer wins, and accounts are usable again afterwards
