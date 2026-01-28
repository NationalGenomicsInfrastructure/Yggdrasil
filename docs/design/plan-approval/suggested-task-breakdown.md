Suggested breakdown:

1.	Plan document model & serialization
	* Plan.to_dict() / from_dict()
	* plan DB client
	* overwrite semantics
	* timestamps & token reset

2.	Auto-run & approval policy integration
	* policy evaluation
	* mapping to status
	* realm → core contract

3.	PlanWatcher (execution gate)
	* _changes integration
	* eligibility rule
	* engine invocation
	* executed_run_token writeback

4.	Checkpoint persistence
	* watcher checkpoint doc in yggdrasil
	* recovery logic
	* restart behavior

5.	Genstat contract
	* required fields
	* approval update rules
	* rerun update rules
	* _rev expectations