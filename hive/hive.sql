# You sql follows
create database examdb;

use examdb;

create table escuela
(region String,distrito String,
ciudad String,ID int, nombre String,
nivel String, serie int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH 'escuelasPR.csv' OVERWRITE INTO TABLE escuela;

create table estudiantes
(region String,distrito String,
IDescuela int, nombre String,
nivel String, sex String, IDestudiante  int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH 'studentsPR.csv' OVERWRITE INTO TABLE escuela;

select region, ciudad,count(*) as conteo from escuela
group by region,ciudad

select escuela.nombre, count(*) from escuela join estudiantes
    on escuela.ID = estudiantes.IDescuela
group by IDescuela;

select count(*) from escuela join estudiantes
    on escuela.ID = estudiantes.IDescuela
  where ciudad = 'Ponce' or ciudad= 'Cabo Rojo';

  select escuela.region, ciudad,count(*) as conteo
from escuela join estudiantes
    on escuela.ID = estudiantes.IDescuela
group by escuela.region,ciudad
