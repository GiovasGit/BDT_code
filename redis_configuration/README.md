This readme file is related to only setting up Redis for Big Data Technology code in steps that should be reproducible. 
This is **assuming we have to do a fresh start**, so **Redis is not installed (or not properly installed and then removed)**, and **we are using an IDE such as PyCharm or similar**.
Steps to reproduce. Be patient because it took a bit, but in around half an hour (errors included) you should be able to do something.

- Open the project repository into your IDE of choice, from Linux or virtual machine Linux if possible. If not, I will put links to official documentation for other OS users. 

- Open the terminal inside of the IDE of choice. Set a Python interpreter for Python 3.5 or later versions. I use Py 3.11 . 

- Install Rust on your machine. It servers the purpose to set up RedisJSON correctly. Documentation here https://www.rust-lang.org/tools/install . The following code takes a few minutes to finish. In the terminal, send
  
     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

- Check that it was installed correctly or seek assistance if it didn't, with this in the terminal:

     rustc --version 

- Start building with the following: 
 
    cargo build --release
    
- Run a test: 

    cargo test

- Run a redis-server with the correct module loaded. Official documentation here https://github.com/RedisJSON/RedisJSON , you can skip docker parts. On Linux: 
  
    redis-server --loadmodule ./target/release/librejson.so
    
- Go into the redis_retrieval.py script, then use start it. 