use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_created_at(time: SystemTime) -> u64 {
   time
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
pub(crate) mod test_utils {
   use std::path::PathBuf;
   use uuid::Uuid;

   pub fn get_file(name: Option<String>, uuid: bool) -> PathBuf {
      let name = name.unwrap_or(String::from("file"));
      let uuid = if uuid {
         Uuid::new_v4().to_string()
      } else {
         String::from("x")
      };

      std::env::current_dir()
          .unwrap()
          .join(format!("./test_cases/{}_{}.bin", name, uuid))
   }

}