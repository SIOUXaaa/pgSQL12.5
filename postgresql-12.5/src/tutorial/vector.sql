
CREATE FUNCTION vector_in(cstring)
    RETURNS vector
    AS '/home/sercoi/PGDev/postgresql-12.5/src/tutorial/vector'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_out(vector)
    RETURNS cstring
    AS '/home/sercoi/PGDev/postgresql-12.5/src/tutorial/vector'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE vector (
    input = vector_in,
    output = vector_out
);


CREATE FUNCTION vector_size(vector)
    RETURNS int4
    AS '/home/sercoi/PGDev/postgresql-12.5/src/tutorial/vector'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_distance(vector, vector)
    RETURNS float4
    AS '/home/sercoi/PGDev/postgresql-12.5/src/tutorial/vector'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_add(vector, vector)
    RETURNS vector
    AS '/home/sercoi/PGDev/postgresql-12.5/src/tutorial/vector'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION vector_sub(vector, vector)
    RETURNS vector
    AS '/home/sercoi/PGDev/postgresql-12.5/src/tutorial/vector'
    LANGUAGE C IMMUTABLE STRICT;


CREATE OPERATOR <#> (
    rightarg = vector,
    procedure = vector_size
);

CREATE OPERATOR <-> (
    leftarg = vector,
    rightarg = vector,
    procedure = vector_distance
);

CREATE OPERATOR + (
    leftarg = vector,
    rightarg = vector,
    procedure = vector_add,
    commutator = +
);

CREATE OPERATOR - (
    leftarg = vector,
    rightarg = vector,
    procedure = vector_sub,
    commutator = -
);


