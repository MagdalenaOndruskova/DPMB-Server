# WAZE DATA ANALYSIS (backend application)

This project was created as Master's thesis at the Faculty of Information Technology, 
Brno university of Technology. 

The main purpose of this project was to analyze and visualize traffic 
data collected from users using navigation application Waze. 
This application was created in cooperation with [data.brno.cz](https://data.brno.cz/)


In this repository you can find a code for running backend application. 
This application is reading data from Waze, using API of city Brno. 
It takes data, performs needed operations and return prepared data that are 
later used in web application. 

Datasets can be found at: [Traffic delays](https://data.brno.cz/datasets/mestobrno::plynulost-dopravy-traffic-delays/about) and [Traffic events](https://data.brno.cz/datasets/mestobrno::ud%C3%A1losti-na-cest%C3%A1ch-traffic-events/about). 

Repository for frontend application can be found here [waze-data-analysis](https://github.com/MagdalenaOndruskova/waze-data-analysis).

Final application is available at: 
- localhost under address `localhost/waze-data-analysis/`
- testing deployment at [data.brno](https://data.brno.cz/apps/70b6c168c69e4955a354622b3e92dd49/explore)

__________________________________
### Usage
In terminal, use command:
```
docker compose up --build
```

___________________________________
### License 
This project is licensed under MIT License.
____________________________________

