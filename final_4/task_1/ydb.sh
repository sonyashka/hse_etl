ydb `
--endpoint grpcs://ydb.serverless.yandexcloud.net:2135 `
--database /ru-central1/b1gjsp13v6dasrvajai1/etnlqlo2v76nlu6aaqm7 `
--sa-key-file authorized_key.json `
import file csv `
--path mental_health `
--delimiter "," `
--skip-rows 1 `
mh_transformed.csv