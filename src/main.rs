use bson::oid::ObjectId;
use bson::{doc, from_document};
use csv::{ReaderBuilder, WriterBuilder};
use encoding_rs::GBK;
use futures::TryStreamExt;
use mongodb::{Client, Collection};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum UserSex {
    Male,
    Female,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct User {
    pub _id: ObjectId,
    pub id: String,
    pub name: String,
    pub group: Vec<ObjectId>,
    password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Time {
    pub on_campus: f64,
    pub off_campus: f64,
    pub social_practice: f64,
    pub total: f64,
}

async fn export() {
    let config =
        serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string("config.json").unwrap())
            .unwrap();
    let uri = config["server"].as_str().unwrap();
    let client = Client::with_uri_str(uri).await.unwrap();
    let db = client.database("zvms");

    let mut df = df!(
        "_id" => &["".to_string()],
        "id" => &["0".to_string()],
        "name" => &["Example".to_string()],
        "class" => &["".to_string()],
        "on_campus" => &[0.0],
        "off_campus" => &[0.0],
        "social_practice" => &[0.0],
        "total" => &[0.0]
    )
    .unwrap();

    let users_collection: Collection<User> = db.collection("users");
    let activities_collection: Collection<()> = db.collection("activities");

    let mut users = users_collection.find(doc! {}, None).await.unwrap();

    while let Some(doc) = users.try_next().await.unwrap() {
        let pipeline = vec![
            doc! {
                "$match": {
                    "$or": [
                        { "members._id": doc._id.clone() },
                        { "members._id": doc._id.to_hex() }
                    ]
                }
            },
            doc! {
                "$unwind": "$members"
            },
            doc! {
                "$match": {
                    "$or": [
                        { "members._id": doc._id.clone() },
                        { "members._id": doc._id.to_hex() }
                    ]
                }
            },
            doc! {
                "$group": {
                    "_id": "$members.mode",
                    "totalDuration": { "$sum": "$members.duration" }
                }
            },
            doc! {
                "$group": {
                    "_id": null,
                    "on_campus": {
                        "$sum": {
                            "$cond": [{ "$eq": ["$_id", "on-campus"] }, "$totalDuration", 0.0]
                        }
                    },
                    "off_campus": {
                        "$sum": {
                            "$cond": [{ "$eq": ["$_id", "off-campus"] }, "$totalDuration", 0.0]
                        }
                    },
                    "social_practice": {
                        "$sum": {
                            "$cond": [{ "$eq": ["$_id", "social-practice"] }, "$totalDuration", 0.0]
                        }
                    },
                    "total": { "$sum": "$totalDuration" }
                }
            },
            doc! {
                "$project": {
                    "_id": 0,
                    "on_campus": 1,
                    "off_campus": 1,
                    "social_practice": 1,
                    "total": 1
                }
            },
        ];

        let mut cursor = activities_collection
            .aggregate(pipeline, None)
            .await
            .unwrap();
        let result = cursor.try_next().await.unwrap();
        if let Some(result) = result {
            let result: Time = from_document(result).unwrap();
            let series_vec = vec![
                Series::new("_id", vec![doc._id.clone().to_hex()]),
                Series::new("id", vec![doc.id.clone()]),
                Series::new("name", vec![doc.name.clone()]),
                Series::new("class", vec!["".to_string()]),
                Series::new("on_campus", vec![result.on_campus]),
                Series::new("off_campus", vec![result.off_campus]),
                Series::new("social_practice", vec![result.social_practice]),
                Series::new("total", vec![result.total]),
            ];
            let extend = DataFrame::new(series_vec).unwrap();
            df.extend(&extend).unwrap();
        }
    }
    println!("{:#?}", df.clone());
    let mut file = std::fs::File::create("output.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df.clone()).unwrap();
}

fn convert() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "output.csv"; // Path to your UTF-8 encoded CSV
    let output_path = "gbk.csv"; // Path for the GB2312 encoded CSV

    // Open the input file
    let mut reader = ReaderBuilder::new().from_path(file_path)?;

    // Create output file
    let mut writer = WriterBuilder::new().from_writer(File::create(output_path)?);

    for result in reader.records() {
        let record = result?;

        // Convert each field from UTF-8 to GB2312
        let converted: Vec<_> = record
            .iter()
            .map(|field| {
                let (cow, _, _) = GBK.encode(field);
                cow.into_owned()
            })
            .collect();

        // Write the converted record to the output file
        writer.write_record(&converted)?;
    }

    Ok(())
}

fn to_excel() {
    let gil = Python::acquire_gil();
    let py = gil.python();

    // Execute Python code
    let code = r#"
import pandas as pd

def read_and_save_csv(input_path, output_path):
    df = pd.read_csv(input_path, encoding='utf-8')
    df.to_excel(output_path, index=False)
"#;

    // Run the Python code
    py.run(code, None, None).unwrap();

    // Use the defined Python function
    let locals = [("pd", py.import("pandas").unwrap())].into_py_dict(py);
    let func: Py<PyAny> = py
        .eval("read_and_save_csv", None, Some(locals))
        .unwrap()
        .extract()
        .unwrap();
    func.call1(py, ("output.csv", "output.xlsx")).unwrap();
}

// async fn convert_to_gb2312() {
//     // let content = std::fs::read_to_string("output.csv").unwrap();
//     let string = "你好，世界！".to_string();
//     match convert_utf8_to_gb2312(string.as_str()) {
//         Ok(output) => {
//             // println!("{:?}", output);
//             let mut file = std::fs::File::create("output2.csv").unwrap();
//             file.write_all(&output).unwrap();
//         }
//         Err(e) => {
//             eprintln!("Error: {}", e);
//         }
//     }
// }

#[tokio::main]
async fn main() {
    let _ = to_excel();
}
