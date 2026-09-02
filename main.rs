
/*
there is two to use SSE or stream...
  1. use only tokio library(Recomended)-> tokio,tokio_stream
  2.use other libraries(not for production)
*/
//the second way...
use std::time::Duration;
use actix_web::{get, http::header, App, HttpResponse, HttpServer, Responder};
use tokio::time::sleep;

#[get("/stream-count")]
async fn stream_count() -> impl Responder {
    // 1. WHAT IT IS: The stream macro initiating our data pipeline.
    //    WHY IT EXISTS: It acts as the "Producer". It compiles our loop into a Rust Stream type.
    //    IF OMITTED: You cannot use the "yield" keyword to emit sequential chunks.
    let count_stream = async_stream::stream! {
        
        // 2. WHAT IT IS: A standard procedural iterator loop.
        //    WHY IT EXISTS: It defines the boundaries of our data generation lifecycle.
        //    IF OMITTED: The stream would have to be coded as repetitive, hardcoded blocks.
        for count in 1..=5 {
            
            // 3. WHAT IT IS: An asynchronous non-blocking timer.
            //    WHY IT EXISTS: It yields control back to Tokio's runtime for 1 second.
            //    IF OMITTED: The loop completes instantly; numbers 1 to 5 blast out instantly.
            sleep(Duration::from_secs(1)).await;
            
            // 4. WHAT IT IS: A formatted SSE standard data chunk layout string.
            //    WHY IT EXISTS: The W3C browser standard strictly requires "data: <content>\n\n".
            //    IF OMITTED/MALFORMED: The browser's Javascript EventSource API will completely ignore the packet.
            let payload = format!("data: {}\n\n", count);
            
            // 5. WHAT IT IS: Conversion of a String into a static memory Byte container.
            //    WHY IT EXISTS: Network cards stream bytes, not high-level abstract Rust types.
            //    IF OMITTED: The stream cannot cross safe asynchronous thread boundaries.
            let byte_chunk = actix_web::web::Bytes::from(payload);
            
            // 6. WHAT IT IS: Emitting a single safe Result chunk out of the loop.
            //    WHY IT EXISTS: Actix demands a Result type so it can detect and handle mid-stream network errors.
            //    IF OMITTED: The stream terminates prematurely because Actix won't know if the data is error-free.
            yield Ok::<_, actix_web::Error>(byte_chunk);
        }
    }; // The macro pipeline closes here.

    // 7. WHAT IT IS: Building a fresh, customized HTTP Response object.
    //    WHY IT EXISTS: To establish the server headers BEFORE flushing out streaming data chunks.
    //    IF OMITTED: The browser assumes you are returning standard text or HTML instead of a stream.
    HttpResponse::Ok()
        // 8. WHAT IT IS: Configuring the SSE protocol header.
        //    WHY IT EXISTS: Tells the browser's networking layer: "Do not wait for a final length, read this continuously".
        //    IF OMITTED: The browser client buffers all numbers and displays nothing until the connection closes.
        .content_type("text/event-stream")

        // 9. WHAT IT IS: Explicit proxy/browser cache override headers.
        //    WHY IT EXISTS: Prevents intermediary proxies (like Nginx, Cloudflare, or local browsers) from storing chunks.
        //    IF OMITTED: Network routers along the way might accumulate your numbers and break the 1-second delay execution.
        .insert_header((header::CACHE_CONTROL, "no-cache"))
        .insert_header((header::CONNECTION, "keep-alive"))

        // 10. WHAT IT IS: The HTTP stream consumer injector.
        //     WHY IT EXISTS: Binds our "count_stream" macro pipeline directly into the active TCP socket output engine.
        //     IF OMITTED: Actix forces you to return a static body allocation (like a string or json object).
        .streaming(count_stream)
}

/*
the core tools->
  1.tokio::time::sleep


*/

