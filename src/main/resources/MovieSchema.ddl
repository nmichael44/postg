drop table if exists movieActor;
drop table if exists movieDirector;
drop table if exists movies;
drop table if exists directors;
drop table if exists actors;
drop table if exists userPermissions;
drop table if exists systemUsers;
drop table if exists permissions;

create table actors(
  actorId bigserial primary key,
  firstName text not null,
  lastName text not null,
  dob date not null
);

create table directors(
  directorId bigserial primary key,
  firstName text not null,
  lastName text not null,
  dob date not null
);

create table movies (
    movieId bigserial primary key,
    title text not null,
    year integer not null
);

create table movieDirector (
  movieId bigint references movies(movieId),
  directorId bigint references directors(directorId),
  primary key (movieId, directorId)
);

create table movieActor (
  movieId bigint references movies(movieId),
  actorId bigint references actors(actorId),
  primary key (movieId, actorId)
);

insert into actors (firstname, lastname, dob) values('Neo', 'Michael', '1970-04-19');

insert into directors (firstname, lastname, dob) values
('Steven', 'Spielberg', '1965-05-01'),
('Neo', 'Michael', '1970-04-19'),
('Neo', 'Momonedes', '2014-01-19');

insert into movies (title, year) values('Xorkatikes malakies', 1980), ('Tsioftes', 2010);

insert into movieDirector values(1, 2),(2, 2);

create table systemUsers(userid bigSerial primary key, loginName text not null, hashedPassword text not null);

insert into systemUsers values(1, 'neo', '$argon2id$v=19$m=65536,t=3,p=1$qc8c8UvrMQRHmDk+lsXohA$orC/dGXZ27Ys3cy+vm44WpOs+UAydtcwDBhSd9ix2Mk');

create table permissions (permissionId bigSerial primary key, description text not null unique);

insert into permissions (description) values('ReadDirectors');
insert into permissions (description) values('ReadActors');
insert into permissions (description) values('ReadMovies');

create table userPermissions(
  userId bigInt not null,
  permissionId bigInt not null,

  PRIMARY KEY (userId, permissionId),

  FOREIGN KEY (userId) REFERENCES systemUsers(userId),
  FOREIGN KEY (permissionId) REFERENCES permissions(permissionId)
);

insert into userPermissions values (1, 1),(1, 2);
