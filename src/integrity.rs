use crate::UpgraderError;
use crate::db_tracker::AppliedUpgrader;
use crate::schema_loader::SchemaUpgrader;

/// Verifies the integrity of the database schema by comparing file-based upgraders with applied ones.
///
/// This function assumes that both `files_upgraders` and `db_upgraders` are sorted by `file_id`
/// and `upgrader_id` in ascending order.
pub fn verify_integrity(
    files_upgraders: &[SchemaUpgrader],
    db_upgraders: &[AppliedUpgrader],
) -> Result<(), UpgraderError> {
    // Verify chronological order of application
    let mut prev_applied_on = None;
    for db_u in db_upgraders {
        if let Some(prev) = prev_applied_on
            && db_u.applied_on < prev
        {
            return Err(UpgraderError::IntegrityError(format!(
                "Upgrader {}:{} was applied at {}, which is before the previous upgrader ({})",
                db_u.file_id, db_u.upgrader_id, db_u.applied_on, prev
            )));
        }
        prev_applied_on = Some(db_u.applied_on);
    }

    let mut files_iter = files_upgraders.iter();
    let mut db_iter = db_upgraders.iter();

    loop {
        let f = files_iter.next();
        let d = db_iter.next();

        match (f, d) {
            (Some(file_u), Some(db_u)) => {
                // 1. Check IDs
                if file_u.file_id != db_u.file_id || file_u.upgrader_id != db_u.upgrader_id {
                    // Mismatch. Determine the type of error.
                    // Compare (file_id, upgrader_id) tuples
                    let file_tuple = (file_u.file_id, file_u.upgrader_id);
                    let db_tuple = (db_u.file_id, db_u.upgrader_id);

                    if file_tuple < db_tuple {
                        // File has an upgrader that is "before" the current DB upgrader.
                        // Since we traverse in order, this means the DB skipped this upgrader.
                        return Err(UpgraderError::IntegrityError(format!(
                            "Gap detected in database migrations. File upgrader {}:{} is missing in database, but later upgrader {}:{} is present.",
                            file_u.file_id, file_u.upgrader_id, db_u.file_id, db_u.upgrader_id
                        )));
                    } else {
                        // File tuple > DB tuple.
                        // This means the DB has an upgrader that is "before" the current File upgrader,
                        // but we didn't see it in the Files list (otherwise we would have matched it previously).
                        return Err(UpgraderError::IntegrityError(format!(
                            "Database contains an upgrader {}:{} that is missing from the migration files.",
                            db_u.file_id, db_u.upgrader_id
                        )));
                    }
                }

                // 2. Check Content
                if file_u.text.trim() != db_u.text.trim() {
                    return Err(UpgraderError::IntegrityError(format!(
                        "Upgrader {}:{}. SQL content has changed.",
                        file_u.file_id, file_u.upgrader_id
                    )));
                }

                if file_u.description.trim() != db_u.description.trim() {
                    return Err(UpgraderError::IntegrityError(format!(
                        "Upgrader {}:{}. Description has changed.\nFile: '{}'\nDB:   '{}'",
                        file_u.file_id, file_u.upgrader_id, file_u.description, db_u.description
                    )));
                }
            }
            (Some(_), None) => {
                // More files than DB. This is normal (pending migrations).
                return Ok(());
            }
            (None, Some(_db_u)) => {
                // More DB than files. This implies the codebase is older than the DB.
                // However, we must ensure that we didn't just 'run out' of files while the DB continued
                // sequentially. If the DB has {0:0, 0:1, 0:2} and files has {0:0, 0:1}, that implies 0:2 was deleted from files.
                // The prompt says: "The only mismatch we allow are that the files are new and the database is old... If the database is new and the files are old (but they agree on the subset and there are no gaps in the middle) that's ok too."

                // If we are here, it means the subset matched perfectly so far.
                // So the files are a strict prefix of the DB. This is valid per the requirements.
                return Ok(());
            }
            (None, None) => {
                // Both finished. Exact match.
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_schema_upgrader(
        file_id: i32,
        upgrader_id: i32,
        text: &str,
        desc: &str,
    ) -> SchemaUpgrader {
        SchemaUpgrader {
            file_id,
            upgrader_id,
            description: desc.to_string(),
            text: text.to_string(),
        }
    }

    fn create_applied_upgrader(
        file_id: i32,
        upgrader_id: i32,
        text: &str,
        desc: &str,
    ) -> AppliedUpgrader {
        AppliedUpgrader {
            file_id,
            upgrader_id,
            description: desc.to_string(),
            text: text.to_string(),
            applied_on: Utc::now(),
        }
    }

    /// User Story: Happy path where migration files and database state match exactly.
    #[test]
    fn test_integrity_happy_path_exact_match() {
        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(0, 1, "SQL2", "Desc2"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL1", "Desc1"),
            create_applied_upgrader(0, 1, "SQL2", "Desc2"),
        ];
        assert!(verify_integrity(&files, &db).is_ok());
    }

    #[test]
    fn test_integrity_happy_path_pending_migrations() {
        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(0, 1, "SQL2", "Desc2"),
            create_schema_upgrader(1, 0, "SQL3", "Desc3"),
        ];
        let db = vec![create_applied_upgrader(0, 0, "SQL1", "Desc1")];
        assert!(verify_integrity(&files, &db).is_ok());
    }

    #[test]
    fn test_integrity_happy_path_db_ahead_files_subset() {
        // This is the "Files are old" case, but they match the prefix.
        let files = vec![create_schema_upgrader(0, 0, "SQL1", "Desc1")];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL1", "Desc1"),
            create_applied_upgrader(0, 1, "SQL2", "Desc2"),
        ];
        assert!(verify_integrity(&files, &db).is_ok());
    }

    #[test]
    fn test_integrity_fail_description_changed() {
        let files = vec![create_schema_upgrader(0, 0, "SQL1", "New Desc")];
        let db = vec![create_applied_upgrader(0, 0, "SQL1", "Old Desc")];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(msg.contains("Description has changed")),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_text_changed() {
        let files = vec![create_schema_upgrader(0, 0, "New SQL", "Desc1")];
        let db = vec![create_applied_upgrader(0, 0, "Old SQL", "Desc1")];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(msg.contains("SQL content has changed")),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_reordered_files() {
        // Developer swaps file 0 and 1 content roughly (or IDs).
        // Files: (0,0)->A, (1,0)->B
        // DB:    (0,0)->B, (1,0)->A
        // This manifests as content mismatch on (0,0) first.
        let files = vec![create_schema_upgrader(0, 0, "SQL_A", "Desc_A")];
        let db = vec![create_applied_upgrader(0, 0, "SQL_B", "Desc_B")];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(
                msg.contains("SQL content has changed") || msg.contains("Description has changed")
            ),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_moved_upgrader_between_files() {
        // Upgrader moved from 0:1 to 1:0
        // DB has 0:1. File has 1:0.
        // Files: (0,0), (1,0)
        // DB:    (0,0), (0,1)

        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(1, 0, "SQL2", "Desc2"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL1", "Desc1"),
            create_applied_upgrader(0, 1, "SQL2", "Desc2"),
        ];

        let err = verify_integrity(&files, &db).unwrap_err();
        // It compares (1,0) from files with (0,1) from DB.
        // (1,0) > (0,1). So DB has an upgrader "before" the current file upgrader.
        match err {
            UpgraderError::IntegrityError(msg) => assert!(msg.contains(
                "Database contains an upgrader 0:1 that is missing from the migration files"
            )),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_changed_file_id() {
        // File 0 becomes File 1.
        // Files: (1,0)
        // DB:    (0,0)
        let files = vec![create_schema_upgrader(1, 0, "SQL", "Desc")];
        let db = vec![create_applied_upgrader(0, 0, "SQL", "Desc")];

        let err = verify_integrity(&files, &db).unwrap_err();
        // (1,0) > (0,0). DB has earlier upgrader.
        match err {
            UpgraderError::IntegrityError(msg) => {
                assert!(msg.contains("Database contains an upgrader 0:0 that is missing"))
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_changed_upgrader_id() {
        // 0:0 becomes 0:1
        // Files: (0,1)
        // DB:    (0,0)
        let files = vec![create_schema_upgrader(0, 1, "SQL", "Desc")];
        let db = vec![create_applied_upgrader(0, 0, "SQL", "Desc")];

        let err = verify_integrity(&files, &db).unwrap_err();
        // (0,1) > (0,0)
        match err {
            UpgraderError::IntegrityError(msg) => {
                assert!(msg.contains("Database contains an upgrader 0:0 that is missing"))
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_inserted_at_start() {
        // Developer adds new upgrader at 0:0. Old 0:0 becomes 0:1.
        // Files: (0,0-New), (0,1-Old)
        // DB:    (0,0-Old)
        let files = vec![
            create_schema_upgrader(0, 0, "SQL_New", "Desc_New"),
            create_schema_upgrader(0, 1, "SQL_Old", "Desc_Old"),
        ];
        let db = vec![create_applied_upgrader(0, 0, "SQL_Old", "Desc_Old")];

        // Mismatch at (0,0). Content differs.
        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(
                msg.contains("SQL content has changed") || msg.contains("Description has changed")
            ),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_inserted_in_middle_file_gap() {
        // Files: (0,0), (0,1-New), (0,2-Old)
        // DB:    (0,0), (0,1-Old) -> Wait, if IDs shift, then DB 0:1 is Old, File 0:1 is New. Mismatch.

        // Scenario: Developer inserts new upgrader, shifts IDs.
        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(0, 1, "SQL_New", "Desc_New"),
            create_schema_upgrader(0, 2, "SQL2", "Desc2"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL1", "Desc1"),
            create_applied_upgrader(0, 1, "SQL2", "Desc2"), // This was 0:1 before shift
        ];

        // At 0:1, content mismatch.
        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(
                msg.contains("SQL content has changed") || msg.contains("Description has changed")
            ),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_inserted_in_middle_missing_in_db() {
        // Scenario: Developer adds 0:1, but keeps 0:2 (assuming no shift - rare manual edit).
        // Files: (0,0), (0,1), (0,2)
        // DB:    (0,0), (0,2)  <-- Missing 0:1

        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(0, 1, "SQL2", "Desc2"),
            create_schema_upgrader(0, 2, "SQL3", "Desc3"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL1", "Desc1"),
            create_applied_upgrader(0, 2, "SQL3", "Desc3"),
        ];

        // At 2nd step: File (0,1) vs DB (0,2).
        // (0,1) < (0,2). File is "earlier". Means DB skipped it.
        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(
                msg.contains("Gap detected in database migrations. File upgrader 0:1 is missing")
            ),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_happy_path_add_to_end_of_file_no_subsequent() {
        // Files: (0,0), (0,1)
        // DB:    (0,0)
        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(0, 1, "SQL2", "Desc2"),
        ];
        let db = vec![create_applied_upgrader(0, 0, "SQL1", "Desc1")];
        assert!(verify_integrity(&files, &db).is_ok());
    }

    #[test]
    fn test_integrity_fail_add_to_end_of_file_with_subsequent_exists() {
        // Files: (0,0), (0,1-New), (1,0)
        // DB:    (0,0), (1,0)  <-- DB already has 1:0, so 0:1 is a "gap" effectively because 1:0 > 0:1

        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(0, 1, "SQL_New", "Desc_New"),
            create_schema_upgrader(1, 0, "SQL2", "Desc2"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL1", "Desc1"),
            create_applied_upgrader(1, 0, "SQL2", "Desc2"),
        ];

        // Compare File (0,1) vs DB (1,0).
        // (0,1) < (1,0). Gap detected.
        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(
                msg.contains("Gap detected in database migrations. File upgrader 0:1 is missing")
            ),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_happy_path_new_file() {
        // Files: (0,0), (1,0)
        // DB:    (0,0)
        let files = vec![
            create_schema_upgrader(0, 0, "SQL1", "Desc1"),
            create_schema_upgrader(1, 0, "SQL2", "Desc2"),
        ];
        let db = vec![create_applied_upgrader(0, 0, "SQL1", "Desc1")];
        assert!(verify_integrity(&files, &db).is_ok());
    }

    /// User Story: Developer changed leading/trailing SQL whitespace in an already applied upgrader.
    /// This should now PASS as we trim whitespace.
    #[test]
    fn test_integrity_success_leading_trailing_whitespace_change() {
        let files = vec![create_schema_upgrader(0, 0, "  SQL  ", " Desc ")];
        let db = vec![create_applied_upgrader(0, 0, "SQL", "Desc")];

        assert!(verify_integrity(&files, &db).is_ok());
    }

    /// User Story: Developer changed INTERNAL SQL whitespace. This should still FAIL.
    #[test]
    fn test_integrity_fail_internal_whitespace_change() {
        let files = vec![create_schema_upgrader(0, 0, "SELECT  1", "Desc")];
        let db = vec![create_applied_upgrader(0, 0, "SELECT 1", "Desc")];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(msg.contains("SQL content has changed")),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_case_sensitivity() {
        let files = vec![create_schema_upgrader(0, 0, "SELECT 1", "Desc")];
        let db = vec![create_applied_upgrader(0, 0, "select 1", "Desc")];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => assert!(msg.contains("SQL content has changed")),
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_multiple_gaps_finds_first() {
        // Files: (0,0), (0,1), (0,2), (0,3)
        // DB:    (0,0), (0,3)
        // Missing (0,1) and (0,2). Should report (0,1).
        let files = vec![
            create_schema_upgrader(0, 0, "SQL0", "Desc0"),
            create_schema_upgrader(0, 1, "SQL1", "Desc1"),
            create_schema_upgrader(0, 2, "SQL2", "Desc2"),
            create_schema_upgrader(0, 3, "SQL3", "Desc3"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL0", "Desc0"),
            create_applied_upgrader(0, 3, "SQL3", "Desc3"),
        ];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => {
                assert!(msg.contains("File upgrader 0:1 is missing"))
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_ghost_upgrader_in_middle() {
        // Files: (0,0), (0,2)  <-- Missing 0:1
        // DB:    (0,0), (0,1), (0,2)
        // Scenario: Developer deleted 0:1 from the file on disk.
        let files = vec![
            create_schema_upgrader(0, 0, "SQL0", "Desc0"),
            create_schema_upgrader(0, 2, "SQL2", "Desc2"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL0", "Desc0"),
            create_applied_upgrader(0, 1, "SQL1", "Desc1"),
            create_applied_upgrader(0, 2, "SQL2", "Desc2"),
        ];

        let err = verify_integrity(&files, &db).unwrap_err();
        // File has (0,2). DB has (0,1).
        // (0,2) > (0,1). Means DB has something "earlier".
        match err {
            UpgraderError::IntegrityError(msg) => {
                assert!(msg.contains("Database contains an upgrader 0:1 that is missing"))
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_ghost_file_gap() {
        let files = vec![
            create_schema_upgrader(0, 0, "SQL0", "Desc0"),
            create_schema_upgrader(2, 0, "SQL2", "Desc2"),
        ];
        let db = vec![
            create_applied_upgrader(0, 0, "SQL0", "Desc0"),
            create_applied_upgrader(1, 0, "SQL1", "Desc1"),
            create_applied_upgrader(2, 0, "SQL2", "Desc2"),
        ];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => {
                assert!(msg.contains("Database contains an upgrader 1:0 that is missing"))
            }
            _ => panic!("Unexpected error type"),
        }
    }

    #[test]
    fn test_integrity_fail_applied_on_out_of_order() {
        use chrono::Duration;
        let now = Utc::now();
        let _later = now + Duration::seconds(10);
        let earlier = now - Duration::seconds(10);

        // DB: 0:0 applied NOW. 0:1 applied EARLIER. This is impossible in normal flow.
        let files = vec![
            create_schema_upgrader(0, 0, "SQL", "Desc"),
            create_schema_upgrader(0, 1, "SQL", "Desc"),
        ];
        // Note: db_upgraders passed to verify_integrity are assumed sorted by ID.
        let db = vec![
            AppliedUpgrader {
                file_id: 0,
                upgrader_id: 0,
                description: "Desc".to_string(),
                text: "SQL".to_string(),
                applied_on: now,
            },
            AppliedUpgrader {
                file_id: 0,
                upgrader_id: 1,
                description: "Desc".to_string(),
                text: "SQL".to_string(),
                applied_on: earlier,
            },
        ];

        let err = verify_integrity(&files, &db).unwrap_err();
        match err {
            UpgraderError::IntegrityError(msg) => {
                assert!(msg.contains("Upgrader 0:1 was applied at"))
            }
            _ => panic!("Unexpected error type"),
        }
    }
}
