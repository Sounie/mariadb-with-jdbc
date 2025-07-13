Potential alternative approaches for achieving upsert with version awareness.

- insert with on duplicate key update, guarding each value with version checking
- delete where existing version is less than supplied version, followed by insert ignoring failure
  - deletion would have transaction isolation level sensitivity, as could potentially have the deletion of one version be combined with insertion from another version - leading to the possibility of missing a valid update as the record would exist after a deletion has been applied
