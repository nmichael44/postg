drop table if exists movieActor;
drop table if exists movieDirector;
drop table if exists movies;
drop table if exists directors;
drop table if exists actors;

create table actors(
  actorId bigint primary key,
  firstName text not null,
  lastName text not null,
  dob date not null
);

create table directors(
  directorId bigint primary key,
  firstName text not null,
  lastName text not null,
  dob date not null
);

create table movies(
  movieId bigint primary key,
  title text not null,
  year Integer not null
);

create table movieDirector (
  movieId bigint references movies(movieId),
  directorId bigint references directors(directorId),
  primary key (movieId, directorId)
);

create table movieActor (
  movieId BIGINT REFERENCES movies(movieId),
  actorId BIGINT REFERENCES actors(actorId),
  PRIMARY KEY (movieId, actorId)
);
