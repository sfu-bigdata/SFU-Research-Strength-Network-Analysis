# SFU-Research-Strength-Network-Analysis
This project examines SFU’s research areas of strength, weakness, and growth using OpenAlex data. It includes network analysis to identify key academic collaborations and comparative benchmarking against other institutions. 

## Extracting the data

An ETL.ipynb file is hosted in the root of the project that will interact with the OpenAlex API to extract relevant data relating to SFU and the U15. Running the data can take several hours as OpenAlex limits the frequency of API calls to 10 requests per second with a maximum resultant set of 200 results at a given time. 