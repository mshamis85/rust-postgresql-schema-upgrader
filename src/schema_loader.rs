use crate::UpgraderError;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub(crate) struct SchemaUpgrader {
    pub(crate) file_id: i32,
    pub(crate) upgrader_id: i32,
    pub(crate) description: String,
    pub(crate) text: String,
}

pub(crate) fn load_upgraders(
    upgraders_folder: impl AsRef<Path>,
) -> Result<Vec<SchemaUpgrader>, UpgraderError> {
    let upgraders_folder = upgraders_folder.as_ref();

    if !upgraders_folder.exists() {
        return Err(UpgraderError::LoaderError(format!(
            "Folder does not exist: {:?}",
            upgraders_folder
        )));
    }

    if !upgraders_folder.is_dir() {
        return Err(UpgraderError::LoaderError(format!(
            "Path is not a directory: {:?}",
            upgraders_folder
        )));
    }

    let mut files: Vec<(i32, PathBuf)> = Vec::new();

    for entry in
        fs::read_dir(upgraders_folder).map_err(|e| UpgraderError::LoaderError(e.to_string()))?
    {
        let entry = entry.map_err(|e| UpgraderError::LoaderError(e.to_string()))?;
        let path = entry.path();

        if path.is_dir() {
            return Err(UpgraderError::LoaderError(format!(
                "Nested directory found: {:?}",
                path
            )));
        }

        if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
            // Ignore hidden files
            if filename.starts_with('.') {
                continue;
            }

            // check extension
            let extension = path
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.to_lowercase());
            match extension.as_deref() {
                Some("sql") | Some("ddl") => {}
                _ => continue, // Ignore non-sql/ddl files
            }

            let parts: Vec<&str> = filename.split('_').collect();
            if let Some(first_part) = parts.first() {
                if let Ok(id) = first_part.parse::<i32>() {
                    files.push((id, path));
                } else {
                    return Err(UpgraderError::LoaderError(format!(
                        "File name must start with a number: {:?}",
                        filename
                    )));
                }
            }
        }
    }

    files.sort_by_key(|k| k.0);

    // Validate file IDs are sequential starting from 0
    for (idx, (file_id, path)) in files.iter().enumerate() {
        if *file_id != idx as i32 {
            if *file_id == 0 && idx != 0 {
                return Err(UpgraderError::LoaderError(format!(
                    "Duplicate file ID 0 found: {:?}",
                    path
                )));
            } else if *file_id < idx as i32 {
                return Err(UpgraderError::LoaderError(format!(
                    "Duplicate file ID {} found: {:?}",
                    file_id, path
                )));
            } else {
                return Err(UpgraderError::LoaderError(format!(
                    "Missing file ID {}. Found {} at {:?}",
                    idx, file_id, path
                )));
            }
        }
    }

    let mut upgraders = Vec::new();

    for (file_id, path) in files {
        let content = fs::read_to_string(&path).map_err(|e| {
            UpgraderError::LoaderError(format!("Failed to read file {:?}: {}", path, e))
        })?;
        let lines = content.lines();

        let mut current_upgrader_id: Option<i32> = None;
        let mut current_description: Option<String> = None;
        let mut current_sql = String::new();
        let mut expected_upgrader_id = 0;

        for line in lines {
            if let Some(header_part) = line.strip_prefix("--- ") {
                // If we have a current upgrader, push it
                if let (Some(uid), Some(desc)) = (current_upgrader_id, &current_description) {
                    let trimmed_sql = current_sql.trim().to_string();
                    if !trimmed_sql.is_empty() {
                        upgraders.push(SchemaUpgrader {
                            file_id,
                            upgrader_id: uid,
                            description: desc.trim().to_string(),
                            text: trimmed_sql,
                        });
                    }
                }

                // Reset for next
                current_sql.clear();

                // Parse new header: "--- <id>: <desc>"
                if let Some((id_str, desc_str)) = header_part.split_once(':') {
                    if let Ok(uid) = id_str.trim().parse::<i32>() {
                        if uid != expected_upgrader_id {
                            return Err(UpgraderError::LoaderError(format!(
                                "Invalid upgrader sequence in file {:?}. Expected ID {}, found {}",
                                path, expected_upgrader_id, uid
                            )));
                        }

                        current_upgrader_id = Some(uid);
                        current_description = Some(desc_str.trim().to_string());
                        expected_upgrader_id += 1;
                    } else {
                        return Err(UpgraderError::LoaderError(format!(
                            "Invalid upgrader ID format in file {:?}: {}",
                            path, line
                        )));
                    }
                } else {
                    return Err(UpgraderError::LoaderError(format!(
                        "Invalid upgrader header format in file {:?}: {}",
                        path, line
                    )));
                }
            } else {
                current_sql.push_str(line);
                current_sql.push('\n');
            }
        }

        // Push the last upgrader
        if let (Some(uid), Some(desc)) = (current_upgrader_id, current_description) {
            let trimmed_sql = current_sql.trim().to_string();
            if !trimmed_sql.is_empty() {
                upgraders.push(SchemaUpgrader {
                    file_id,
                    upgrader_id: uid,
                    description: desc.trim().to_string(),
                    text: trimmed_sql,
                });
            }
        }
    }

    Ok(upgraders)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    /// User Story: Happy path. Developer provides correctly named files with sequential IDs and valid content.
    #[test]
    fn test_load_upgraders_success() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 0: Create users\nCREATE TABLE users (id INT);").unwrap();
        writeln!(
            f0,
            "--- 1: Add email\nALTER TABLE users ADD COLUMN email TEXT;"
        )
        .unwrap();

        let file1 = folder.join("001_orders.sql");
        let mut f1 = File::create(file1).unwrap();
        writeln!(f1, "--- 0: Create orders\nCREATE TABLE orders (id INT);").unwrap();

        let result = load_upgraders(folder).unwrap();
        assert_eq!(result.len(), 3);

        assert_eq!(result[0].file_id, 0);
        assert_eq!(result[0].upgrader_id, 0);
        assert_eq!(result[0].description, "Create users");
        assert_eq!(result[0].text, "CREATE TABLE users (id INT);");

        assert_eq!(result[1].file_id, 0);
        assert_eq!(result[1].upgrader_id, 1);
        assert_eq!(result[1].description, "Add email");
        assert_eq!(result[1].text, "ALTER TABLE users ADD COLUMN email TEXT;");

        assert_eq!(result[2].file_id, 1);
        assert_eq!(result[2].upgrader_id, 0);
        assert_eq!(result[2].description, "Create orders");
        assert_eq!(result[2].text, "CREATE TABLE orders (id INT);");
    }

    /// User Story: Developer organizes migrations in subdirectories (Not allowed).
    #[test]
    fn test_load_upgraders_nested_dir_fails() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        fs::create_dir(folder.join("nested")).unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("Nested directory found")),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer provides a file that does not start with a number.
    #[test]
    fn test_load_upgraders_invalid_filename_fails() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        File::create(folder.join("not_a_number_init.sql")).unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => {
                assert!(e.contains("File name must start with a number"))
            }
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer uses an invalid header format for an upgrader step.
    #[test]
    fn test_load_upgraders_invalid_header_fails() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- not_an_id: Description\nSQL;").unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(
                e.contains("Invalid upgrader ID format")
                    || e.contains("Invalid upgrader header format")
            ),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer's first file does not start at ID 0.
    #[test]
    fn test_load_upgraders_file_id_not_start_at_zero() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        File::create(folder.join("001_init.sql")).unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("Missing file ID 0")),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer leaves a gap in the file ID sequence (e.g., 000, 002).
    #[test]
    fn test_load_upgraders_file_id_gap() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        File::create(folder.join("000_init.sql")).unwrap();
        File::create(folder.join("002_more.sql")).unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("Missing file ID 1")),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer has duplicate file IDs (e.g., 000_a.sql, 000_b.sql).
    #[test]
    fn test_load_upgraders_file_id_duplicate() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        File::create(folder.join("000_init.sql")).unwrap();
        File::create(folder.join("000_dup.sql")).unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("Duplicate file ID 0")),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer leaves a gap in the upgrader step sequence within a file.
    #[test]
    fn test_load_upgraders_upgrader_id_sequence_error() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 0: Step 0\nSQL;").unwrap();
        writeln!(f0, "--- 2: Step 2\nSQL;").unwrap(); // Skipped 1

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("Invalid upgrader sequence")),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer's first upgrader in a file does not start at ID 0.
    #[test]
    fn test_load_upgraders_upgrader_id_not_start_zero() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 1: Step 1\nSQL;").unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("Invalid upgrader sequence")),
            _ => panic!("Expected LoaderError"),
        }
    }

    /// User Story: Developer includes a file with a non-SQL extension (e.g., .txt).
    /// The loader should IGNORE it.
    #[test]
    fn test_load_upgraders_non_sql_extension() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_readme.txt");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 0: README\nThis is just text.").unwrap();

        let result = load_upgraders(folder).unwrap();
        assert_eq!(result.len(), 0);
    }

    /// User Story: Developer creates an upgrader with no SQL content (empty block).
    /// Current behavior: The upgrader is skipped.
    #[test]
    fn test_load_upgraders_empty_sql_block_skipped() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 0: Empty\n\n--- 1: Real\nSELECT 1;").unwrap();

        let result = load_upgraders(folder).unwrap();

        // ID 0 is skipped because text is empty. ID 1 is loaded.
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].upgrader_id, 1);
    }

    /// User Story: Developer uses .ddl or uppercase .SQL extensions.
    #[test]
    fn test_load_upgraders_extensions_allowed() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.ddl");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 0: DDL\nSELECT 1;").unwrap();

        let file1 = folder.join("001_upper.SQL");
        let mut f1 = File::create(file1).unwrap();
        writeln!(f1, "--- 0: SQL\nSELECT 2;").unwrap();

        let result = load_upgraders(folder).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].file_id, 0);
        assert_eq!(result[1].file_id, 1);
    }

    /// User Story: Developer writes upgraders out of order (e.g., 0, then 2).
    /// This is caught because we enforce strict sequential increment (0, 1, 2...).
    #[test]
    fn test_load_upgraders_out_of_order_fails() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        // 0 is correct. 2 is wrong (expected 1).
        writeln!(f0, "--- 0: First\nSELECT 1;").unwrap();
        writeln!(f0, "--- 2: Wrong\nSELECT 2;").unwrap();
        writeln!(f0, "--- 1: Late\nSELECT 3;").unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(
                e.contains("Invalid upgrader sequence") && e.contains("Expected ID 1, found 2")
            ),
            _ => panic!("Expected LoaderError"),
        }
    }
}
