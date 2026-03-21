export const data_folder_path = process.env.DATA_FOLDER_PATH || './data'
export const bucket_root_path = `${data_folder_path}/buckets`
export const bucket_ids_path = `${data_folder_path}/set-bucket-ids.bin`
export const storage_version = 1
export const snapshot_magic = 'BDB1'
export const result_magic = 'RSLT'
