use std::fs::File;

// use arrow::util::display::array_value_to_string;
// use arrow::record_batch::RecordBatch;
// use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use leptos::prelude::*;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use web_sys::window;

// pub fn read_parquet_file(path: &str) -> Vec<RecordBatch> {
//     let file = File::open(path).unwrap();
//     let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
//         .unwrap()
//         .build()
//         .unwrap();
//     let mut batches = Vec::new();
//     for batch in parquet_reader {
//         batches.push(batch.unwrap());
//     }
//     batches
// }

// pub fn record_batches_to_string_rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
//     let mut result = Vec::new();

//     for batch in batches {
//         let columns = batch.columns();
//         let num_rows = batch.num_rows();

//         for row_idx in 0..num_rows {
//             let mut row = Vec::new();
//             for col in columns {
//                 let value = if col.is_null(row_idx) {
//                     "null".to_string()
//                 } else {
//                     array_value_to_string(col, row_idx)
//                         .unwrap_or_else(|_| "<?>".to_string())
//                 };
//                 row.push(value);
//             }
//             result.push(row);
//         }
//     }

//     result
// }

#[component]
pub fn DataTable(records: Vec<Vec<String>>) -> impl IntoView {
    let header: Vec<String> = records.get(0).cloned().unwrap_or_default();
    let rows_source = {
        move || records.iter().skip(1).cloned().collect::<Vec<_>>()
    };

    view! {
        <table class="min-w-full border border-gray-300">
            <thead>
                <tr>
                    <For
                        each=move || header.clone()
                        key=|h| h.clone()
                        children=move |col| view! {
                            <th class="px-2 py-1 border-b">{col}</th>
                        }
                    />
                </tr>
            </thead>
            <tbody>
                <For
                    each=rows_source
                    key=|row| row.join(",")
                    children=move |row| view! {
                        <tr>
                            <For
                                each=move || row.clone()
                                key=|cell| cell.clone()
                                children=move |cell| view! {
                                    <td class="px-2 py-1 border-b">{cell}</td>
                                }
                            />
                        </tr>
                    }
                />
            </tbody>
        </table>
    }
}

pub async fn write_to_clipboard(text: &str) -> Result<(), JsValue> {
    let window = window().ok_or_else(|| JsValue::from_str("No global `window` exists"))?;
    let navigator = window.navigator();
    let clipboard = navigator.clipboard();
    let promise = clipboard.write_text(text);
    JsFuture::from(promise).await?;
    Ok(())
}
