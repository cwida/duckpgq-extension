/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
# define YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    IDENT = 258,                   /* IDENT  */
    FCONST = 259,                  /* FCONST  */
    SCONST = 260,                  /* SCONST  */
    BCONST = 261,                  /* BCONST  */
    XCONST = 262,                  /* XCONST  */
    Op = 263,                      /* Op  */
    ICONST = 264,                  /* ICONST  */
    PARAM = 265,                   /* PARAM  */
    TYPECAST = 266,                /* TYPECAST  */
    DOT_DOT = 267,                 /* DOT_DOT  */
    COLON_EQUALS = 268,            /* COLON_EQUALS  */
    EQUALS_GREATER = 269,          /* EQUALS_GREATER  */
    POWER_OF = 270,                /* POWER_OF  */
    LAMBDA_ARROW = 271,            /* LAMBDA_ARROW  */
    DOUBLE_ARROW = 272,            /* DOUBLE_ARROW  */
    LESS_EQUALS = 273,             /* LESS_EQUALS  */
    GREATER_EQUALS = 274,          /* GREATER_EQUALS  */
    NOT_EQUALS = 275,              /* NOT_EQUALS  */
    ABORT_P = 276,                 /* ABORT_P  */
    ABSOLUTE_P = 277,              /* ABSOLUTE_P  */
    ACCESS = 278,                  /* ACCESS  */
    ACTION = 279,                  /* ACTION  */
    ACYCLIC = 280,                 /* ACYCLIC  */
    ADD_P = 281,                   /* ADD_P  */
    ADMIN = 282,                   /* ADMIN  */
    AFTER = 283,                   /* AFTER  */
    AGGREGATE = 284,               /* AGGREGATE  */
    ALL = 285,                     /* ALL  */
    ALSO = 286,                    /* ALSO  */
    ALTER = 287,                   /* ALTER  */
    ALWAYS = 288,                  /* ALWAYS  */
    ANALYSE = 289,                 /* ANALYSE  */
    ANALYZE = 290,                 /* ANALYZE  */
    AND = 291,                     /* AND  */
    ANTI = 292,                    /* ANTI  */
    ANY = 293,                     /* ANY  */
    ARE = 294,                     /* ARE  */
    ARRAY = 295,                   /* ARRAY  */
    AS = 296,                      /* AS  */
    ASC_P = 297,                   /* ASC_P  */
    ASSERTION = 298,               /* ASSERTION  */
    ASSIGNMENT = 299,              /* ASSIGNMENT  */
    ASYMMETRIC = 300,              /* ASYMMETRIC  */
    AT = 301,                      /* AT  */
    ATTACH = 302,                  /* ATTACH  */
    ATTRIBUTE = 303,               /* ATTRIBUTE  */
    AUTHORIZATION = 304,           /* AUTHORIZATION  */
    BACKWARD = 305,                /* BACKWARD  */
    BEFORE = 306,                  /* BEFORE  */
    BEGIN_P = 307,                 /* BEGIN_P  */
    BETWEEN = 308,                 /* BETWEEN  */
    BIGINT = 309,                  /* BIGINT  */
    BINARY = 310,                  /* BINARY  */
    BIT = 311,                     /* BIT  */
    BOOLEAN_P = 312,               /* BOOLEAN_P  */
    BOTH = 313,                    /* BOTH  */
    BY = 314,                      /* BY  */
    CACHE = 315,                   /* CACHE  */
    CALL_P = 316,                  /* CALL_P  */
    CALLED = 317,                  /* CALLED  */
    CASCADE = 318,                 /* CASCADE  */
    CASCADED = 319,                /* CASCADED  */
    CASE = 320,                    /* CASE  */
    CAST = 321,                    /* CAST  */
    CATALOG_P = 322,               /* CATALOG_P  */
    CHAIN = 323,                   /* CHAIN  */
    CHAR_P = 324,                  /* CHAR_P  */
    CHARACTER = 325,               /* CHARACTER  */
    CHARACTERISTICS = 326,         /* CHARACTERISTICS  */
    CHECK_P = 327,                 /* CHECK_P  */
    CHECKPOINT = 328,              /* CHECKPOINT  */
    CLASS = 329,                   /* CLASS  */
    CLOSE = 330,                   /* CLOSE  */
    CLUSTER = 331,                 /* CLUSTER  */
    COALESCE = 332,                /* COALESCE  */
    COLLATE = 333,                 /* COLLATE  */
    COLLATION = 334,               /* COLLATION  */
    COLUMN = 335,                  /* COLUMN  */
    COLUMNS = 336,                 /* COLUMNS  */
    COMMENT = 337,                 /* COMMENT  */
    COMMENTS = 338,                /* COMMENTS  */
    COMMIT = 339,                  /* COMMIT  */
    COMMITTED = 340,               /* COMMITTED  */
    COMPRESSION = 341,             /* COMPRESSION  */
    CONCURRENTLY = 342,            /* CONCURRENTLY  */
    CONFIGURATION = 343,           /* CONFIGURATION  */
    CONFLICT = 344,                /* CONFLICT  */
    CONNECTION = 345,              /* CONNECTION  */
    CONSTRAINT = 346,              /* CONSTRAINT  */
    CONSTRAINTS = 347,             /* CONSTRAINTS  */
    CONTENT_P = 348,               /* CONTENT_P  */
    CONTINUE_P = 349,              /* CONTINUE_P  */
    CONVERSION_P = 350,            /* CONVERSION_P  */
    COPY = 351,                    /* COPY  */
    COST = 352,                    /* COST  */
    CREATE_P = 353,                /* CREATE_P  */
    CROSS = 354,                   /* CROSS  */
    CSV = 355,                     /* CSV  */
    CUBE = 356,                    /* CUBE  */
    CURRENT_P = 357,               /* CURRENT_P  */
    CURRENT_CATALOG = 358,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 359,            /* CURRENT_DATE  */
    CURRENT_ROLE = 360,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 361,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 362,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 363,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 364,            /* CURRENT_USER  */
    CURSOR = 365,                  /* CURSOR  */
    CYCLE = 366,                   /* CYCLE  */
    DATA_P = 367,                  /* DATA_P  */
    DATABASE = 368,                /* DATABASE  */
    DAY_P = 369,                   /* DAY_P  */
    DAYS_P = 370,                  /* DAYS_P  */
    DEALLOCATE = 371,              /* DEALLOCATE  */
    DEC = 372,                     /* DEC  */
    DECIMAL_P = 373,               /* DECIMAL_P  */
    DECLARE = 374,                 /* DECLARE  */
    DEFAULT = 375,                 /* DEFAULT  */
    DEFAULTS = 376,                /* DEFAULTS  */
    DEFERRABLE = 377,              /* DEFERRABLE  */
    DEFERRED = 378,                /* DEFERRED  */
    DEFINER = 379,                 /* DEFINER  */
    DELETE_P = 380,                /* DELETE_P  */
    DELIMITER = 381,               /* DELIMITER  */
    DELIMITERS = 382,              /* DELIMITERS  */
    DEPENDS = 383,                 /* DEPENDS  */
    DESC_P = 384,                  /* DESC_P  */
    DESCRIBE = 385,                /* DESCRIBE  */
    DESTINATION = 386,             /* DESTINATION  */
    DETACH = 387,                  /* DETACH  */
    DICTIONARY = 388,              /* DICTIONARY  */
    DISABLE_P = 389,               /* DISABLE_P  */
    DISCARD = 390,                 /* DISCARD  */
    DISTINCT = 391,                /* DISTINCT  */
    DO = 392,                      /* DO  */
    DOCUMENT_P = 393,              /* DOCUMENT_P  */
    DOMAIN_P = 394,                /* DOMAIN_P  */
    DOUBLE_P = 395,                /* DOUBLE_P  */
    DROP = 396,                    /* DROP  */
    EACH = 397,                    /* EACH  */
    EDGE = 398,                    /* EDGE  */
    ELEMENT_ID = 399,              /* ELEMENT_ID  */
    ELSE = 400,                    /* ELSE  */
    ENABLE_P = 401,                /* ENABLE_P  */
    ENCODING = 402,                /* ENCODING  */
    ENCRYPTED = 403,               /* ENCRYPTED  */
    END_P = 404,                   /* END_P  */
    ENUM_P = 405,                  /* ENUM_P  */
    ESCAPE = 406,                  /* ESCAPE  */
    EVENT = 407,                   /* EVENT  */
    EXCEPT = 408,                  /* EXCEPT  */
    EXCLUDE = 409,                 /* EXCLUDE  */
    EXCLUDING = 410,               /* EXCLUDING  */
    EXCLUSIVE = 411,               /* EXCLUSIVE  */
    EXECUTE = 412,                 /* EXECUTE  */
    EXISTS = 413,                  /* EXISTS  */
    EXPLAIN = 414,                 /* EXPLAIN  */
    EXPORT_P = 415,                /* EXPORT_P  */
    EXPORT_STATE = 416,            /* EXPORT_STATE  */
    EXTENSION = 417,               /* EXTENSION  */
    EXTERNAL = 418,                /* EXTERNAL  */
    EXTRACT = 419,                 /* EXTRACT  */
    FALSE_P = 420,                 /* FALSE_P  */
    FAMILY = 421,                  /* FAMILY  */
    FETCH = 422,                   /* FETCH  */
    FILTER = 423,                  /* FILTER  */
    FIRST_P = 424,                 /* FIRST_P  */
    FLOAT_P = 425,                 /* FLOAT_P  */
    FOLLOWING = 426,               /* FOLLOWING  */
    FOR = 427,                     /* FOR  */
    FORCE = 428,                   /* FORCE  */
    FOREIGN = 429,                 /* FOREIGN  */
    FORWARD = 430,                 /* FORWARD  */
    FREEZE = 431,                  /* FREEZE  */
    FROM = 432,                    /* FROM  */
    FULL = 433,                    /* FULL  */
    FUNCTION = 434,                /* FUNCTION  */
    FUNCTIONS = 435,               /* FUNCTIONS  */
    GENERATED = 436,               /* GENERATED  */
    GLOB = 437,                    /* GLOB  */
    GLOBAL = 438,                  /* GLOBAL  */
    GRANT = 439,                   /* GRANT  */
    GRANTED = 440,                 /* GRANTED  */
    GRAPH = 441,                   /* GRAPH  */
    GRAPH_TABLE = 442,             /* GRAPH_TABLE  */
    GROUP_P = 443,                 /* GROUP_P  */
    GROUPING = 444,                /* GROUPING  */
    GROUPING_ID = 445,             /* GROUPING_ID  */
    GROUPS = 446,                  /* GROUPS  */
    HANDLER = 447,                 /* HANDLER  */
    HAVING = 448,                  /* HAVING  */
    HEADER_P = 449,                /* HEADER_P  */
    HOLD = 450,                    /* HOLD  */
    HOUR_P = 451,                  /* HOUR_P  */
    HOURS_P = 452,                 /* HOURS_P  */
    IDENTITY_P = 453,              /* IDENTITY_P  */
    IF_P = 454,                    /* IF_P  */
    IGNORE_P = 455,                /* IGNORE_P  */
    ILIKE = 456,                   /* ILIKE  */
    IMMEDIATE = 457,               /* IMMEDIATE  */
    IMMUTABLE = 458,               /* IMMUTABLE  */
    IMPLICIT_P = 459,              /* IMPLICIT_P  */
    IMPORT_P = 460,                /* IMPORT_P  */
    IN_P = 461,                    /* IN_P  */
    INCLUDE_P = 462,               /* INCLUDE_P  */
    INCLUDING = 463,               /* INCLUDING  */
    INCREMENT = 464,               /* INCREMENT  */
    INDEX = 465,                   /* INDEX  */
    INDEXES = 466,                 /* INDEXES  */
    INHERIT = 467,                 /* INHERIT  */
    INHERITS = 468,                /* INHERITS  */
    INITIALLY = 469,               /* INITIALLY  */
    INLINE_P = 470,                /* INLINE_P  */
    INNER_P = 471,                 /* INNER_P  */
    INOUT = 472,                   /* INOUT  */
    INPUT_P = 473,                 /* INPUT_P  */
    INSENSITIVE = 474,             /* INSENSITIVE  */
    INSERT = 475,                  /* INSERT  */
    INSTALL = 476,                 /* INSTALL  */
    INSTEAD = 477,                 /* INSTEAD  */
    INT_P = 478,                   /* INT_P  */
    INTEGER = 479,                 /* INTEGER  */
    INTERSECT = 480,               /* INTERSECT  */
    INTERVAL = 481,                /* INTERVAL  */
    INTO = 482,                    /* INTO  */
    INVOKER = 483,                 /* INVOKER  */
    IS = 484,                      /* IS  */
    ISNULL = 485,                  /* ISNULL  */
    ISOLATION = 486,               /* ISOLATION  */
    JOIN = 487,                    /* JOIN  */
    JSON = 488,                    /* JSON  */
    KEEP = 489,                    /* KEEP  */
    KEY = 490,                     /* KEY  */
    LABEL = 491,                   /* LABEL  */
    LANGUAGE = 492,                /* LANGUAGE  */
    LARGE_P = 493,                 /* LARGE_P  */
    LAST_P = 494,                  /* LAST_P  */
    LATERAL_P = 495,               /* LATERAL_P  */
    LEADING = 496,                 /* LEADING  */
    LEAKPROOF = 497,               /* LEAKPROOF  */
    LEFT = 498,                    /* LEFT  */
    LEVEL = 499,                   /* LEVEL  */
    LIKE = 500,                    /* LIKE  */
    LIMIT = 501,                   /* LIMIT  */
    LISTEN = 502,                  /* LISTEN  */
    LOAD = 503,                    /* LOAD  */
    LOCAL = 504,                   /* LOCAL  */
    LOCALTIME = 505,               /* LOCALTIME  */
    LOCALTIMESTAMP = 506,          /* LOCALTIMESTAMP  */
    LOCATION = 507,                /* LOCATION  */
    LOCK_P = 508,                  /* LOCK_P  */
    LOCKED = 509,                  /* LOCKED  */
    LOGGED = 510,                  /* LOGGED  */
    MACRO = 511,                   /* MACRO  */
    MAP = 512,                     /* MAP  */
    MAPPING = 513,                 /* MAPPING  */
    MATCH = 514,                   /* MATCH  */
    MATERIALIZED = 515,            /* MATERIALIZED  */
    MAXVALUE = 516,                /* MAXVALUE  */
    METHOD = 517,                  /* METHOD  */
    MICROSECOND_P = 518,           /* MICROSECOND_P  */
    MICROSECONDS_P = 519,          /* MICROSECONDS_P  */
    MILLISECOND_P = 520,           /* MILLISECOND_P  */
    MILLISECONDS_P = 521,          /* MILLISECONDS_P  */
    MINUTE_P = 522,                /* MINUTE_P  */
    MINUTES_P = 523,               /* MINUTES_P  */
    MINVALUE = 524,                /* MINVALUE  */
    MODE = 525,                    /* MODE  */
    MONTH_P = 526,                 /* MONTH_P  */
    MONTHS_P = 527,                /* MONTHS_P  */
    MOVE = 528,                    /* MOVE  */
    NAME_P = 529,                  /* NAME_P  */
    NAMES = 530,                   /* NAMES  */
    NATIONAL = 531,                /* NATIONAL  */
    NATURAL = 532,                 /* NATURAL  */
    NCHAR = 533,                   /* NCHAR  */
    NEW = 534,                     /* NEW  */
    NEXT = 535,                    /* NEXT  */
    NO = 536,                      /* NO  */
    NODE = 537,                    /* NODE  */
    NONE = 538,                    /* NONE  */
    NOT = 539,                     /* NOT  */
    NOTHING = 540,                 /* NOTHING  */
    NOTIFY = 541,                  /* NOTIFY  */
    NOTNULL = 542,                 /* NOTNULL  */
    NOWAIT = 543,                  /* NOWAIT  */
    NULL_P = 544,                  /* NULL_P  */
    NULLIF = 545,                  /* NULLIF  */
    NULLS_P = 546,                 /* NULLS_P  */
    NUMERIC = 547,                 /* NUMERIC  */
    OBJECT_P = 548,                /* OBJECT_P  */
    OF = 549,                      /* OF  */
    OFF = 550,                     /* OFF  */
    OFFSET = 551,                  /* OFFSET  */
    OIDS = 552,                    /* OIDS  */
    OLD = 553,                     /* OLD  */
    ON = 554,                      /* ON  */
    ONLY = 555,                    /* ONLY  */
    OPERATOR = 556,                /* OPERATOR  */
    OPTION = 557,                  /* OPTION  */
    OPTIONS = 558,                 /* OPTIONS  */
    OR = 559,                      /* OR  */
    ORDER = 560,                   /* ORDER  */
    ORDINALITY = 561,              /* ORDINALITY  */
    OUT_P = 562,                   /* OUT_P  */
    OUTER_P = 563,                 /* OUTER_P  */
    OVER = 564,                    /* OVER  */
    OVERLAPS = 565,                /* OVERLAPS  */
    OVERLAY = 566,                 /* OVERLAY  */
    OVERRIDING = 567,              /* OVERRIDING  */
    OWNED = 568,                   /* OWNED  */
    OWNER = 569,                   /* OWNER  */
    PARALLEL = 570,                /* PARALLEL  */
    PARSER = 571,                  /* PARSER  */
    PARTIAL = 572,                 /* PARTIAL  */
    PARTITION = 573,               /* PARTITION  */
    PASSING = 574,                 /* PASSING  */
    PASSWORD = 575,                /* PASSWORD  */
    PATH = 576,                    /* PATH  */
    PATHS = 577,                   /* PATHS  */
    PERCENT = 578,                 /* PERCENT  */
    PIVOT = 579,                   /* PIVOT  */
    PIVOT_LONGER = 580,            /* PIVOT_LONGER  */
    PIVOT_WIDER = 581,             /* PIVOT_WIDER  */
    PLACING = 582,                 /* PLACING  */
    PLANS = 583,                   /* PLANS  */
    POLICY = 584,                  /* POLICY  */
    POSITION = 585,                /* POSITION  */
    POSITIONAL = 586,              /* POSITIONAL  */
    PRAGMA_P = 587,                /* PRAGMA_P  */
    PRECEDING = 588,               /* PRECEDING  */
    PRECISION = 589,               /* PRECISION  */
    PREPARE = 590,                 /* PREPARE  */
    PREPARED = 591,                /* PREPARED  */
    PRESERVE = 592,                /* PRESERVE  */
    PRIMARY = 593,                 /* PRIMARY  */
    PRIOR = 594,                   /* PRIOR  */
    PRIVILEGES = 595,              /* PRIVILEGES  */
    PROCEDURAL = 596,              /* PROCEDURAL  */
    PROCEDURE = 597,               /* PROCEDURE  */
    PROGRAM = 598,                 /* PROGRAM  */
    PROPERTIES = 599,              /* PROPERTIES  */
    PROPERTY = 600,                /* PROPERTY  */
    PUBLICATION = 601,             /* PUBLICATION  */
    QUALIFY = 602,                 /* QUALIFY  */
    QUOTE = 603,                   /* QUOTE  */
    RANGE = 604,                   /* RANGE  */
    READ_P = 605,                  /* READ_P  */
    REAL = 606,                    /* REAL  */
    REASSIGN = 607,                /* REASSIGN  */
    RECHECK = 608,                 /* RECHECK  */
    RECURSIVE = 609,               /* RECURSIVE  */
    REF = 610,                     /* REF  */
    REFERENCES = 611,              /* REFERENCES  */
    REFERENCING = 612,             /* REFERENCING  */
    REFRESH = 613,                 /* REFRESH  */
    REINDEX = 614,                 /* REINDEX  */
    RELATIONSHIP = 615,            /* RELATIONSHIP  */
    RELATIVE_P = 616,              /* RELATIVE_P  */
    RELEASE = 617,                 /* RELEASE  */
    RENAME = 618,                  /* RENAME  */
    REPEATABLE = 619,              /* REPEATABLE  */
    REPLACE = 620,                 /* REPLACE  */
    REPLICA = 621,                 /* REPLICA  */
    RESET = 622,                   /* RESET  */
    RESPECT_P = 623,               /* RESPECT_P  */
    RESTART = 624,                 /* RESTART  */
    RESTRICT = 625,                /* RESTRICT  */
    RETURNING = 626,               /* RETURNING  */
    RETURNS = 627,                 /* RETURNS  */
    REVOKE = 628,                  /* REVOKE  */
    RIGHT = 629,                   /* RIGHT  */
    ROLE = 630,                    /* ROLE  */
    ROLLBACK = 631,                /* ROLLBACK  */
    ROLLUP = 632,                  /* ROLLUP  */
    ROW = 633,                     /* ROW  */
    ROWS = 634,                    /* ROWS  */
    RULE = 635,                    /* RULE  */
    SAMPLE = 636,                  /* SAMPLE  */
    SAVEPOINT = 637,               /* SAVEPOINT  */
    SCHEMA = 638,                  /* SCHEMA  */
    SCHEMAS = 639,                 /* SCHEMAS  */
    SCROLL = 640,                  /* SCROLL  */
    SEARCH = 641,                  /* SEARCH  */
    SECOND_P = 642,                /* SECOND_P  */
    SECONDS_P = 643,               /* SECONDS_P  */
    SECURITY = 644,                /* SECURITY  */
    SELECT = 645,                  /* SELECT  */
    SEMI = 646,                    /* SEMI  */
    SEQUENCE = 647,                /* SEQUENCE  */
    SEQUENCES = 648,               /* SEQUENCES  */
    SERIALIZABLE = 649,            /* SERIALIZABLE  */
    SERVER = 650,                  /* SERVER  */
    SESSION = 651,                 /* SESSION  */
    SESSION_USER = 652,            /* SESSION_USER  */
    SET = 653,                     /* SET  */
    SETOF = 654,                   /* SETOF  */
    SETS = 655,                    /* SETS  */
    SHARE = 656,                   /* SHARE  */
    SHORTEST = 657,                /* SHORTEST  */
    SHOW = 658,                    /* SHOW  */
    SIMILAR = 659,                 /* SIMILAR  */
    SIMPLE = 660,                  /* SIMPLE  */
    SKIP = 661,                    /* SKIP  */
    SMALLINT = 662,                /* SMALLINT  */
    SNAPSHOT = 663,                /* SNAPSHOT  */
    SOME = 664,                    /* SOME  */
    SOURCE = 665,                  /* SOURCE  */
    SQL_P = 666,                   /* SQL_P  */
    STABLE = 667,                  /* STABLE  */
    STANDALONE_P = 668,            /* STANDALONE_P  */
    START = 669,                   /* START  */
    STATEMENT = 670,               /* STATEMENT  */
    STATISTICS = 671,              /* STATISTICS  */
    STDIN = 672,                   /* STDIN  */
    STDOUT = 673,                  /* STDOUT  */
    STORAGE = 674,                 /* STORAGE  */
    STORED = 675,                  /* STORED  */
    STRICT_P = 676,                /* STRICT_P  */
    STRIP_P = 677,                 /* STRIP_P  */
    STRUCT = 678,                  /* STRUCT  */
    SUBSCRIPTION = 679,            /* SUBSCRIPTION  */
    SUBSTRING = 680,               /* SUBSTRING  */
    SUMMARIZE = 681,               /* SUMMARIZE  */
    SYMMETRIC = 682,               /* SYMMETRIC  */
    SYSID = 683,                   /* SYSID  */
    SYSTEM_P = 684,                /* SYSTEM_P  */
    TABLE = 685,                   /* TABLE  */
    TABLES = 686,                  /* TABLES  */
    TABLESAMPLE = 687,             /* TABLESAMPLE  */
    TABLESPACE = 688,              /* TABLESPACE  */
    TEMP = 689,                    /* TEMP  */
    TEMPLATE = 690,                /* TEMPLATE  */
    TEMPORARY = 691,               /* TEMPORARY  */
    TEXT_P = 692,                  /* TEXT_P  */
    THEN = 693,                    /* THEN  */
    TIME = 694,                    /* TIME  */
    TIMESTAMP = 695,               /* TIMESTAMP  */
    TO = 696,                      /* TO  */
    TRAIL = 697,                   /* TRAIL  */
    TRAILING = 698,                /* TRAILING  */
    TRANSACTION = 699,             /* TRANSACTION  */
    TRANSFORM = 700,               /* TRANSFORM  */
    TREAT = 701,                   /* TREAT  */
    TRIGGER = 702,                 /* TRIGGER  */
    TRIM = 703,                    /* TRIM  */
    TRUE_P = 704,                  /* TRUE_P  */
    TRUNCATE = 705,                /* TRUNCATE  */
    TRUSTED = 706,                 /* TRUSTED  */
    TRY_CAST = 707,                /* TRY_CAST  */
    TYPE_P = 708,                  /* TYPE_P  */
    TYPES_P = 709,                 /* TYPES_P  */
    UNBOUNDED = 710,               /* UNBOUNDED  */
    UNCOMMITTED = 711,             /* UNCOMMITTED  */
    UNENCRYPTED = 712,             /* UNENCRYPTED  */
    UNION = 713,                   /* UNION  */
    UNIQUE = 714,                  /* UNIQUE  */
    UNKNOWN = 715,                 /* UNKNOWN  */
    UNLISTEN = 716,                /* UNLISTEN  */
    UNLOGGED = 717,                /* UNLOGGED  */
    UNPIVOT = 718,                 /* UNPIVOT  */
    UNTIL = 719,                   /* UNTIL  */
    UPDATE = 720,                  /* UPDATE  */
    USE_P = 721,                   /* USE_P  */
    USER = 722,                    /* USER  */
    USING = 723,                   /* USING  */
    VACUUM = 724,                  /* VACUUM  */
    VALID = 725,                   /* VALID  */
    VALIDATE = 726,                /* VALIDATE  */
    VALIDATOR = 727,               /* VALIDATOR  */
    VALUE_P = 728,                 /* VALUE_P  */
    VALUES = 729,                  /* VALUES  */
    VARCHAR = 730,                 /* VARCHAR  */
    VARIADIC = 731,                /* VARIADIC  */
    VARYING = 732,                 /* VARYING  */
    VERBOSE = 733,                 /* VERBOSE  */
    VERSION_P = 734,               /* VERSION_P  */
    VERTEX = 735,                  /* VERTEX  */
    VIEW = 736,                    /* VIEW  */
    VIEWS = 737,                   /* VIEWS  */
    VIRTUAL = 738,                 /* VIRTUAL  */
    VOLATILE = 739,                /* VOLATILE  */
    WALK = 740,                    /* WALK  */
    WHEN = 741,                    /* WHEN  */
    WHERE = 742,                   /* WHERE  */
    WHITESPACE_P = 743,            /* WHITESPACE_P  */
    WINDOW = 744,                  /* WINDOW  */
    WITH = 745,                    /* WITH  */
    WITHIN = 746,                  /* WITHIN  */
    WITHOUT = 747,                 /* WITHOUT  */
    WORK = 748,                    /* WORK  */
    WRAPPER = 749,                 /* WRAPPER  */
    WRITE_P = 750,                 /* WRITE_P  */
    XML_P = 751,                   /* XML_P  */
    XMLATTRIBUTES = 752,           /* XMLATTRIBUTES  */
    XMLCONCAT = 753,               /* XMLCONCAT  */
    XMLELEMENT = 754,              /* XMLELEMENT  */
    XMLEXISTS = 755,               /* XMLEXISTS  */
    XMLFOREST = 756,               /* XMLFOREST  */
    XMLNAMESPACES = 757,           /* XMLNAMESPACES  */
    XMLPARSE = 758,                /* XMLPARSE  */
    XMLPI = 759,                   /* XMLPI  */
    XMLROOT = 760,                 /* XMLROOT  */
    XMLSERIALIZE = 761,            /* XMLSERIALIZE  */
    XMLTABLE = 762,                /* XMLTABLE  */
    YEAR_P = 763,                  /* YEAR_P  */
    YEARS_P = 764,                 /* YEARS_P  */
    YES_P = 765,                   /* YES_P  */
    ZONE = 766,                    /* ZONE  */
    NOT_LA = 767,                  /* NOT_LA  */
    NULLS_LA = 768,                /* NULLS_LA  */
    WITH_LA = 769,                 /* WITH_LA  */
    POSTFIXOP = 770,               /* POSTFIXOP  */
    UMINUS = 771                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 14 "third_party/libpg_query/grammar/grammar.y"

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;

#line 626 "third_party/libpg_query/grammar/grammar_out.hpp"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif




int base_yyparse (core_yyscan_t yyscanner);


#endif /* !YY_BASE_YY_THIRD_PARTY_LIBPG_QUERY_GRAMMAR_GRAMMAR_OUT_HPP_INCLUDED  */
