select distinct b.id_bus, b.latitude_bus, b.longitude_bus
from business b, review_phoenix r
where b.id_bus = r.bus_id_raz;
