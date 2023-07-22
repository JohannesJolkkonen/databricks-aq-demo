CREATE TABLE air_quality (city_id INT,date DATE,  pollutant VARCHAR(50), concentration   FLOAT, PRIMARY KEY (city_id, date, pollutant));

 INSERT INTO air_quality (city_id,   date,pollutant,concentration) VALUES (1,'2023-07-01', 'PM2.5',10.5),(1, '2023-07-01', 'PM10',20.3),(2,'2023-07-01','PM2.5', 8.2),(2, '2023-07-01','PM10', 15.1),(3, '2023-07-01','PM2.5',12.7),(3, '2023-07-01','PM10',18.6);

CREATE TABLE population( city_id INT,year INT,population INT,PRIMARY KEY   (city_id,year));
INSERT INTO population(city_id,year,population)VALUES(1,2023,1000000),(2,2023,750000),(3,2023,500000);

SELECT a.city_id,a.date, a.pollutant,a.concentration,p.population
FROM   air_quality   a JOIN population   p ON a.city_id = p.city_id WHERE a.date = '2023-07-01'
ORDER BY a.city_id;
