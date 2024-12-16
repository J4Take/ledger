use crate::AccountOperation::*;
use crate::AccountOperationResult::*;
use crate::AccountState::*;
use crate::OperationState::*;
use anyhow::{anyhow, Result};
use csv::{StringRecord,ReaderBuilder, Trim};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::BufReader;

// Record used to deserialize the csv. We map field names to avoid clash with "type" keyword and
// also to assign something nicer.
#[derive(Debug, serde::Deserialize)]
struct TransactionEntry {
    #[serde(rename = "type")]
    t: String,
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(rename = "tx")]
    uid: u32,
    amount: f32,
}

// This is operation state. We can be in RegularDeposit (after deposit or resolved dispute,
// DisputedDeposit after dispute, FinalDeposit after chargeback or Afterwithdrawal after
// a withdrawal).
#[derive(Clone,Copy,Debug)]
enum OperationState {
    RegularDeposit { amount: f32 }, // After Deposit or after Deposit -> Dispute -> Resolve
    DisputedDeposit { amount: f32 }, // After Deposit -> Dispute
    FinalDeposit,   // After Deposit -> Chargeback
    AfterWithdrawal, // After Withdrawal
}

// This is AccountState - the account can either be open (for normal operation) or locked (after a
// chargeback).
#[derive(Debug)]
enum AccountState {
    Open { available: f32, held: f32 }, // Normal operation
    Locked { available: f32, held: f32 }, // Chargeback happened, corresponding operation is in
                                        // FinalDeposit OperationState
}

// AccountOperation - reflecting the original operation.
#[derive(Debug)]
enum AccountOperation {
    Deposit { amount: f32 },
    Withdrawal { amount: f32 },
    Dispute,
    Resolve,
    Chargeback,
}

// Account, including its state.
#[derive(Debug)]
struct Account {
    state: AccountState,
    oplog: HashMap<u32, OperationState>, // This is a map of transaction id -> OperationState
}

// Ledger - the map of all accounts, by their respective client_id.
#[derive(Debug)]
struct Ledger {
    accounts: HashMap<u16, Account>, // This is a map of client_id -> Account
}

// The result of applying an operation on an account.
#[derive(Debug)]
enum AccountOperationResult {
    AppendOperation {
        state: AccountState,
        op: OperationState,
    },
    ModifyOperation {
        state: AccountState,
        op: OperationState,
    },
}

fn process_operation(
    op: AccountOperation,
    op_to_modify: Option<OperationState>,
    a: &mut Account,
) -> Result<AccountOperationResult> {
    // Main state machine. Takes an AccountOperation (representing a current operation), an Option
    // of OperationState, which will be the operation to modify for modifying operations or None
    // for AppendOperations and a mutable account and results in the mutation on the account.
    // Returns AccountOperationResult. It mutates the state of the account, but does not change
    // the oplog. Oplog is then modified in the subsequent function.
    match (&a.state, op_to_modify, op) {
        (Locked { .. }, _, _) => Err(anyhow! {"The account is locked! Skipping transaction"}),
        (Open { available, held }, None, Deposit { amount }) => Ok(AppendOperation {
            op: RegularDeposit { amount: amount },
            state: Open {
                available: *available + amount,
                held: *held,
            },
        }),
        (Open { available, held }, None, Withdrawal { amount }) => {
            if amount > *available {
                Err(anyhow! {"Insufficient funds. Skipping withdrawal"})
            } else {
                Ok(AppendOperation {
                    op: AfterWithdrawal,
                    state: Open {
                        available: *available - amount,
                        held: *held,
                    },
                })
            }
        }
        (Open { available, held }, Some(RegularDeposit { amount }), Dispute) => {
            Ok(ModifyOperation {
                op: DisputedDeposit { amount: amount },
                state: Open {
                    available: *available - amount,
                    held: *held + amount,
                },
            })
        }
        (Open { available, held }, Some(DisputedDeposit { amount }), Resolve) => {
            Ok(ModifyOperation {
                op: RegularDeposit { amount: amount },
                state: Open {
                    available: *available + amount,
                    held: *held - amount,
                },
            })
        }
        (Open { available, held }, Some(DisputedDeposit { amount }), Chargeback) => {
            Ok(ModifyOperation {
                op: FinalDeposit,
                state: Locked {
                    available: *available,
                    held: *held - amount,
                },
            })
        }
        _ => Err(anyhow! {"Illegal state transition. Skipping operation"}),
    }
}

// This function mutates the oplog of a given account by applying the modification
// contained in the AccountOperationResult.
fn apply_result_to_account(
    result: AccountOperationResult,
    tx_id: u32,
    a: &mut Account,
) -> Result<()> {
    match result {
        AppendOperation { state, op } => {
            a.state = state;
            a.oplog.insert(tx_id, op);
            return Ok(());
        }
        ModifyOperation { state, op } => {
            a.state = state;
            a.oplog.get_mut(&tx_id).map(|val| {
                *val = op;
            });
            return Ok(());
        }
    }
}

fn is_transaction_in_log(tx: &TransactionEntry, a: &Account) -> bool {
    a.oplog.contains_key(&tx.uid)
}

fn process_transaction(tx: TransactionEntry, a: &mut Account) -> Result<()> {
    let result: AccountOperationResult;
    match tx.t.as_str() {
        "deposit" => {
            if is_transaction_in_log(&tx, a) {
                return Err(anyhow! {"Duplicate transaction id. Skipping operation"});
            } else {
                result = process_operation(Deposit { amount: tx.amount }, None, a)?;
            }
        }
        "withdrawal" => {
            if is_transaction_in_log(&tx, a) {
                return Err(anyhow! {"Duplicate transaction id. Skipping operation"});
            } else {
                result = process_operation(Withdrawal { amount: tx.amount }, None, a)?;
            }
        }
        "dispute" => {
            if !is_transaction_in_log(&tx, a) {
                return Err(anyhow! {"Transaction not found in log. Skipping operation"});
            } else {
                result = process_operation(Dispute, Some(a.oplog[&tx.uid]), a)?;
            }
        }
        "resolve" => {
            if !is_transaction_in_log(&tx, a) {
                return Err(anyhow! {"Transaction not found in log. Skipping operation"});
            } else {
                result = process_operation(Resolve, Some(a.oplog[&tx.uid]), a)?;
            }
        }
        "chargeback" => {
            if !is_transaction_in_log(&tx, a) {
                return Err(anyhow! {"Transaction not found in log. Skipping operation"});
            } else {
                result =
                    process_operation(Chargeback, Some(a.oplog[&tx.uid]), a)?;
            }
        }
        _ => return Err(anyhow! {"Unknown transaction type. Skipping operation"}),
    }
    apply_result_to_account(result, tx.uid, a)
}

fn apply_transaction(tx: TransactionEntry, l: &mut Ledger) -> Result<()> {
    match l.accounts.get_mut(&tx.client_id) {
        Some(mut account) => process_transaction(tx, &mut account)?,
        _ => {
            let a = Account {
                state: Open {
                    available: 0.0,
                    held: 0.0,
                },
                oplog: HashMap::new(),
            };
            l.accounts.insert(tx.client_id, a);
            // we can unwrap here, because we have just inserted this entry, so if it does not
            // exist, it would mean something is seriously wrong.
            let account = &mut l.accounts.get_mut(&tx.client_id).unwrap();
            process_transaction(tx, account)?;
        }
    }
    Ok(())
}

fn deserialize_transaction_entry(record: Result<StringRecord,csv::Error>) -> Result<TransactionEntry,Box<dyn std::error::Error>> {
    let te: TransactionEntry = record?.deserialize(None)?;
    Ok(te)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Invalid input - should contain name of a transaction file");
        return;
    }
    let transactions_filename = &args[1];

    let mut l = Ledger {
        accounts: HashMap::new(),
    };
    let file = File::open(transactions_filename).unwrap();
    
    let mut rdr = ReaderBuilder::new()
        .flexible(true)
        .trim(Trim::All)
        // BufReader ensures that we don't read in the whole file at once.
        .from_reader(BufReader::new(file));

    // The iterator takes care of reading the file record by record.
    for record in rdr.records() {
        match deserialize_transaction_entry(record) {
        Ok(entry) => if let Err(e) = apply_transaction(entry, &mut l) {
            eprintln!("Error occurred: {}", e);
        },
        Err(e) => eprintln!("Error occurred: {}", e),
        }
    }
    println!("client,available,held,total,locked");
    for (aid, account) in l.accounts.iter() {
        //        println!("Ledger entry {}: {:?}", aid, &account);
        match &account.state {
            Open { available, held } => println!(
                "{},{:.4},{:.4},{:.4},{}",
                aid,
                available,
                held,
                available + held,
                false
            ),
            Locked { available, held } => println!(
                "{},{:.4},{:.4},{:.4},{}",
                aid,
                available,
                held,
                available + held,
                true
            ),
        };
    }
}
