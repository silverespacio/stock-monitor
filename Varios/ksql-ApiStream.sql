/*Crear STREAM*/
CREATE STREAM stock (simbolo VARCHAR, precio DOUBLE, volumen DOUBLE, fecha_hora VARCHAR)
  WITH (kafka_topic='stock-updates', value_format='json', partitions=1);

/**1**/
CREATE TABLE PROMEDIO_X_SIMBOLO AS
SELECT simbolo,
(sum(PRECIO)/COUNT(*)) PRECIO_PROMEDIO
from stock
GROUP BY simbolo
EMIT CHANGES;
Select * from PROMEDIO_X_SIMBOLO;

/**2**/
CREATE TABLE CANTIDAD_X_SIMBOLO AS
SELECT simbolo,
count(*) cantidad_procesada
from stock
GROUP BY simbolo
EMIT CHANGES;
Select * from CANTIDAD_X_SIMBOLO;

/**3**/
CREATE TABLE MAXIMO_X_SIMBOLO AS
SELECT simbolo,
MAX(PRECIO) PRECIO_MAXIMO
from stock
GROUP BY simbolo
EMIT CHANGES;
SELECT * FROM MAXIMO_X_SIMBOLO;

/**4**/
CREATE TABLE MINIMO_X_SIMBOLO AS
SELECT simbolo,
MIN(PRECIO) PRECIO_MINIMO
from stock
GROUP BY simbolo
EMIT CHANGES;
SELECT * FROM MINIMO_X_SIMBOLO;

/*EXTRAS*/
CREATE TABLE RESUMEN_STOCK AS
SELECT simbolo,
count(*) cantidad_procesada,sum(PRECIO) TOTAL_PRECIO,(sum(PRECIO)/COUNT(*)) PROMEDIO,
MIN(PRECIO)PRECIO_MINIMO, MAX(PRECIO) PRECIO_MAXIMO, latest_by_offset(fecha_hora) AS ultimo_mensaje
from stock
GROUP BY simbolo
EMIT CHANGES;
SELECT * FROM RESUMEN_STOCK;
	