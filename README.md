## Schema

#### Peers table

- Peer’s nickname
- Birthday

#### Tasks table

- Name of the task
- Name of the task, which is the entry condition
- Maximum number of XP

To access the task, you must complete the task that is its entry condition.
For simplicity, assume that each task has only one entry condition.
There must be one task in the table that has no entry condition (i.e., the ParentTask field is null).

#### Check status

Create an enumeration type for the check status that contains the following values:
- Start - the check starts
- Success - successful completion of the check
- Failure - unsuccessful completion of the check

#### P2P Table

- ID
- Check ID
- Nickname of the checking peer
- [P2P check status](#check-status)
- Time

Each P2P check consists of 2 table records: the first has a start status, the second has a success or failure status. \
The table cannot contain more than one incomplete P2P check related to a specific task, a peer and a checking peer. \
Each P2P check (i.e. both records of which it consists) refers to the check in the Checks table to which it belongs. 

#### Verter Table

- ID
- Check ID
- [Check status by Verter](#check-status)
- Time

Each check by Verter consists of 2 table records: the first has a start status, the second has a success or failure status. \
Each check by Verter (i.e. both records of which it consists) refers to the check in the Checks table to which it belongs. \
Сheck by Verter can only refer to those checks in the Checks table that already include a successful P2P check.

#### Checks table

- ID
- Peer’s nickname
- Name of the task
- Check date

Describes the check of the task as a whole. The check necessarily includes a **one** P2P step and possibly a Verter step.
For simplicity, assume that peer to peer and autotests related to the same check always happen on the same day.

The check is considered successful if the corresponding P2P step is successful and the Verter step is successful, or if there is no Verter step.
The check is considered a failure if at least one of the steps is unsuccessful. This means that checks in which the P2P step has not yet been completed, or it is successful but the Verter step has not yet been completed, are neither successful nor failed.

#### TransferredPoints table

- ID
- Nickname of the checking peer
- Nickname of the peer being checked
- Number of transferred peer points for all time (only from the one being checked to the checker)

At each P2P check, the peer being checked passes one peer point to the checker.
This table contains all pairs of the peer being checked-the checker and the number of transferred peer points, that is the number of P2P checks of the specified peer by the specified checker.

#### Friends table

- ID
- Nickname of the first peer
- Nickname of the second peer

Friendship is mutual, i.e. the first peer is a friend of the second one, and vice versa.

#### Recommendations table

- ID
- Nickname of the peer
- Nickname of the peer to whom it is recommended to go for the check

Everyone can like how the P2P check was performed by a particular peer. The peer specified in the Peer field recommends passing the P2P check from the peer in the RecommendedPeer field. 
Each peer can recommend either one or several checkers at a time.

#### XP Table

- ID
- Check ID
- Number of XP received

For each successful check, the peer who completes the task receives some amount of XP displayed in this table.
The amount of XP cannot exceed the maximum available number for the task being checked.
The first field of this table can only refer to successful checks.

#### TimeTracking table

- ID
- Peer's nickname
- Date
- Time
- State (1 - in, 2 - out)

This table contains information about peers' visits to campus.
When a peer enters campus, a record is added to the table with state 1, when leaving it adds a record with state 2.