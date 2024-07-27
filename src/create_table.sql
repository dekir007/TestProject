CREATE TABLE public.imoex2 (
                            id serial4 NOT NULL,
                            ticker varchar NOT NULL,
                            per bpchar(1) NOT NULL,
                            "date" date NOT NULL,
                            "time" time NOT NULL,
                            "open" money NOT NULL,
                            high money NOT NULL,
                            low money NOT NULL,
                            "close" money NOT NULL,
                            vol int8 NOT NULL,
                            CONSTRAINT imoex_pk PRIMARY KEY (id)
                        );