use crate::UpgraderError;
use std::path::{Path, PathBuf};
use std::fs;

#[derive(Debug, Clone)]
pub struct SchemaUpgrader {
    pub file_id: i32,
    pub upgrader_id: i32,
    pub description: String,
    pub text: String,
}

pub fn load_upgraders(upgraders_folder: impl AsRef<Path>) -> Result<Vec<SchemaUpgrader>, UpgraderError> {
    let upgraders_folder = upgraders_folder.as_ref();
    
    if !upgraders_folder.exists() {
        return Err(UpgraderError::LoaderError(format!("Folder does not exist: {:?}", upgraders_folder)));
    }

    if !upgraders_folder.is_dir() {
        return Err(UpgraderError::LoaderError(format!("Path is not a directory: {:?}", upgraders_folder)));
    }

    let mut files: Vec<(i32, PathBuf)> = Vec::new();

    for entry in fs::read_dir(upgraders_folder).map_err(|e| UpgraderError::LoaderError(e.to_string()))? {
        let entry = entry.map_err(|e| UpgraderError::LoaderError(e.to_string()))?;
        let path = entry.path();

        if path.is_dir() {
            return Err(UpgraderError::LoaderError(format!("Nested directory found: {:?}", path)));
        }
        
        if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
            // Ignore hidden files or files not starting with a digit
             if filename.starts_with('.') {
                continue;
            }

            let parts: Vec<&str> = filename.split('_').collect();
            if let Some(first_part) = parts.first() {
                if let Ok(id) = first_part.parse::<i32>() {
                    files.push((id, path));
                } else {
                     return Err(UpgraderError::LoaderError(format!("File name must start with a number: {:?}", filename)));
                }
            }
        }
    }

    files.sort_by_key(|k| k.0);

    // Validate file IDs are sequential starting from 0
    for (idx, (file_id, path)) in files.iter().enumerate() {
        if *file_id != idx as i32 {
            if *file_id == 0 && idx != 0 {
                 return Err(UpgraderError::LoaderError(format!("Duplicate file ID 0 found: {:?}", path)));
            } else if *file_id < idx as i32 {
                 return Err(UpgraderError::LoaderError(format!("Duplicate file ID {} found: {:?}", file_id, path)));
            } else {
                 return Err(UpgraderError::LoaderError(format!("Missing file ID {}. Found {} at {:?}", idx, file_id, path)));
            }
        }
    }

    let mut upgraders = Vec::new();

    for (file_id, path) in files {
        let content = fs::read_to_string(&path).map_err(|e| UpgraderError::LoaderError(format!("Failed to read file {:?}: {}", path, e)))?;
        let mut lines = content.lines();
        
        let mut current_upgrader_id: Option<i32> = None;
        let mut current_description: Option<String> = None;
        let mut current_sql = String::new();
        let mut expected_upgrader_id = 0;

        while let Some(line) = lines.next() {
            if line.starts_with("--- ") {
                // If we have a current upgrader, push it
                if let (Some(uid), Some(desc)) = (current_upgrader_id, &current_description) {
                    if !current_sql.trim().is_empty() {
                         upgraders.push(SchemaUpgrader {
                            file_id,
                            upgrader_id: uid,
                            description: desc.clone(),
                            text: current_sql.trim().to_string(),
                        });
                    }
                }

                // Reset for next
                current_sql.clear();

                // Parse new header: "--- <id>: <desc>"
                let header_part = &line[4..]; // Skip "--- "
                if let Some((id_str, desc_str)) = header_part.split_once(':') {
                     if let Ok(uid) = id_str.trim().parse::<i32>() {
                        if uid != expected_upgrader_id {
                            return Err(UpgraderError::LoaderError(format!("Invalid upgrader sequence in file {:?}. Expected ID {}, found {}", path, expected_upgrader_id, uid)));
                        }
                        
                        current_upgrader_id = Some(uid);
                        current_description = Some(desc_str.trim().to_string());
                        expected_upgrader_id += 1;
                     } else {
                         return Err(UpgraderError::LoaderError(format!("Invalid upgrader ID format in file {:?}: {}", path, line)));
                     }
                } else {
                     return Err(UpgraderError::LoaderError(format!("Invalid upgrader header format in file {:?}: {}", path, line)));
                }

            } else {
                current_sql.push_str(line);
                current_sql.push('\n');
            }
        }

        // Push the last upgrader
        if let (Some(uid), Some(desc)) = (current_upgrader_id, current_description) {
             if !current_sql.trim().is_empty() {
                upgraders.push(SchemaUpgrader {
                    file_id,
                    upgrader_id: uid,
                    description: desc,
                    text: current_sql.trim().to_string(),
                });
            }
        }
    }

    Ok(upgraders)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_load_upgraders_success() {
        let dir = tempdir().unwrap();
        let folder = dir.path();

        let file0 = folder.join("000_init.sql");
        let mut f0 = File::create(file0).unwrap();
        writeln!(f0, "--- 0: Create users\nCREATE TABLE users (id INT);").unwrap();
        writeln!(f0, "--- 1: Add email\nALTER TABLE users ADD COLUMN email TEXT;").unwrap();

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

    #[test]
    fn test_load_upgraders_invalid_filename_fails() {
        let dir = tempdir().unwrap();
        let folder = dir.path();
        
        File::create(folder.join("not_a_number_init.sql")).unwrap();

        let result = load_upgraders(folder);
        assert!(result.is_err());
        match result.unwrap_err() {
            UpgraderError::LoaderError(e) => assert!(e.contains("File name must start with a number")),
            _ => panic!("Expected LoaderError"),
        }
    }

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
            UpgraderError::LoaderError(e) => assert!(e.contains("Invalid upgrader ID format") || e.contains("Invalid upgrader header format")),
            _ => panic!("Expected LoaderError"),
        }
    }

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
}
