CREATE TABLE User (
  id_usr MEDIUMINT NOT NULL AUTO_INCREMENT,
  id_original_usr VARCHAR(30) NOT NULL,
  name_usr VARCHAR(50) NOT NULL,
  yelping_since_usr DATE NOT NULL,
  review_count_usr MEDIUMINT NOT NULL,
  fans_usr MEDIUMINT NOT NULL,
  type_usr VARCHAR(20) NOT NULL,
  avg_stars_usr FLOAT(6,5),
  votes_funny_usr MEDIUMINT,
  votes_useful_usr MEDIUMINT,
  votes_cool_usr MEDIUMINT,
  compliments_profile_usr MEDIUMINT,
  compliments_cute_usr MEDIUMINT,
  compliments_funny MEDIUMINT,
  compliments_plain_usr MEDIUMINT,
  compliments_writer_usr MEDIUMINT,
  compliments_note_usr MEDIUMINT,
  compliments_photos_usr MEDIUMINT,
  compliments_hot_usr MEDIUMINT,
  compliments_cool_usr MEDIUMINT,
  compliments_more_usr MEDIUMINT,
  PRIMARY KEY(id_usr)
);

CREATE UNIQUE INDEX idx_id_original_user ON User(id_original_usr);
CREATE INDEX idx_yelping_since_usr ON User(yelping_since_usr);

CREATE TABLE Business (
  id_bus MEDIUMINT NOT NULL AUTO_INCREMENT,
  id_original_bus VARCHAR(30) NOT NULL,
  name_bus VARCHAR(80) NOT NULL,
  type_bus VARCHAR(20) NOT NULL,
  city_bus VARCHAR(50) NOT NULL,
  state_bus VARCHAR(3) NOT NULL,
  address_bus VARCHAR(60),
  open_bus TINYINT(1) NOT NULL,
  review_count_bus MEDIUMINT NOT NULL,
  stars_bus FLOAT(2,1) NOT NULL,
  latitude_bus FLOAT(11,8) NOT NULL,
  longitude_bus FLOAT(11,8) NOT NULL,
  PRIMARY KEY(id_bus)
);

CREATE UNIQUE INDEX idx_id_original_bus ON Business(id_original_bus);
CREATE INDEX idx_city_bus ON Business(city_bus);
CREATE INDEX idx_state_bus ON Business(state_bus);
CREATE INDEX idx_latitude_bus ON Business(latitude_bus);
CREATE INDEX idx_longitude_bus ON Business(longitude_bus);

CREATE TABLE Review_AZ (
  id_raz MEDIUMINT NOT NULL AUTO_INCREMENT,
  id_original_raz VARCHAR(30) NOT NULL,
  user_id_raz MEDIUMINT NOT NULL,
  bus_id_raz MEDIUMINT NOT NULL,
  date_raz DATE NOT NULL,
  type_raz VARCHAR(20) NOT NULL,
  stars_raz FLOAT(2,1) NOT NULL,
  votes_funny_raz MEDIUMINT NOT NULL,
  votes_useful_raz MEDIUMINT NOT NULL,
  votes_cool_raz MEDIUMINT NOT NULL,
  text_raz TEXT NOT NULL,
  PRIMARY KEY(id_raz)
);

CREATE TABLE Review_NV LIKE Review_AZ;
CREATE TABLE Review_NC LIKE Review_AZ;

ALTER TABLE review_az ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id_raz) REFERENCES user(id_usr);
ALTER TABLE review_az ADD CONSTRAINT fk_bus_id FOREIGN KEY (bus_id_raz) REFERENCES business(id_bus);
CREATE INDEX idx_date_raz ON review_az(date_raz);

ALTER TABLE review_nv ADD CONSTRAINT fk_user_id_nv FOREIGN KEY (user_id_raz) REFERENCES user(id_usr);
ALTER TABLE review_nv ADD CONSTRAINT fk_bus_id_nv FOREIGN KEY (bus_id_raz) REFERENCES business(id_bus);
CREATE INDEX idx_date_raz ON review_nv(date_raz);

ALTER TABLE review_nc ADD CONSTRAINT fk_user_id_nc FOREIGN KEY (user_id_raz) REFERENCES user(id_usr);
ALTER TABLE review_nc ADD CONSTRAINT fk_bus_id_nc FOREIGN KEY (bus_id_raz) REFERENCES business(id_bus);
CREATE INDEX idx_date_raz ON review_nc(date_raz);
