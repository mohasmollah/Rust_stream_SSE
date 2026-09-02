
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
  1.tokio::time::sleep-> for sleep and delay for 1s
  2.async_stream::stream! -> the heart, convert thing thing into Stream.
  3.actix_web::web::Bytes -> we need to send Bytes not String or int so we convert them into bytes


*/



//the first thing...
#[get("/stream-channel")]
async fn stream_channel() -> impl Responder {
    // 1. WHAT IT IS: Creating an async internal memory pipe.
    //    WHY IT EXISTS: To bridge our independent background worker with the HTTP response.
    //    IF OMITTED: There is no way to send data out of a background task into Actix.
    let (tx, rx) = tokio::sync::mpsc::channel(10);

    // 2. WHAT IT IS: Handing a task to Tokio's background thread pool.
    //    WHY IT EXISTS: Fires up an independent "worker" that lives separately from the request.
    //    IF OMITTED: The route handler hangs because it tries to do all the work synchronously.
    tokio::spawn(async move {
        for count in 1..=5 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let payload = format!("data: {}\n\n", count);
            let byte_chunk = actix_web::web::Bytes::from(payload);

            // 3. WHAT IT IS: Pushing bytes into the pipe.
            //    WHY IT EXISTS: Delivers the data chunk to the waiting channel receiver.
            //    IF OMITTED/FAILS: If .send() errors out, it means the client closed their browser tab. 
            //                      The `if ... break` cleanly stops the background worker.
            if tx.send(Ok::<_, actix_web::Error>(byte_chunk)).await.is_err() {
                break; 
            }
        }
    }); // The background task runs completely on its own now!

    // 4. WHAT IT IS: Converting a Tokio Channel Receiver into an Actix-compatible Stream.
    //    WHY IT EXISTS: Acts as a translator interface so Actix understands how to pull data from the pipe.
    //    IF OMITTED: Actix throws a compile error: "Receiver does not implement Stream".
    let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // 5. WHAT IT IS: Passing our translated channel directly into the HTTP output socket.
    //    WHY IT EXISTS: Instructs Actix to flush headers instantly, then wait and listen to the channel `rx`.
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .streaming(body_stream)
}



