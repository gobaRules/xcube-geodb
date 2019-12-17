-- Table: public.land_use

-- DROP TABLE public.land_use;

CREATE EXTENSION postgis;

CREATE TABLE public.land_use
(
    id       integer PRIMARY KEY,
    geometry geometry(Geometry, 3794) NOT NULL
);


INSERT INTO public.land_use(id, geometry)
VALUES (1, '0103000020D20E000001000000110000007593188402B51B41B6F3FDD4423FF6405839B4C802B51B412B8716D9EC3EF6406F1283C0EBB41B41A8C64B37C53EF640B6F3FDD4E4B41B419A999999A33EF6400E2DB29DCFB41B41EE7C3F35B63EF6407F6ABC74C0B41B41EE7C3F35B63EF6407B14AE47BDB41B41AAF1D24D043FF6408B6CE77B64B41B413F355EBA8F3FF6402B8716D970B41B41986E1283EC3FF640A4703D0A76B41B4179E92631AE3FF6404260E5D08AB41B4123DBF97E923FF6409EEFA7C69CB41B4100000000AC3FF6405839B448B3B41B411D5A643B973FF6408195438BC6B41B41666666666C3FF640D122DBF9E3B41B4139B4C876383FF640E9263188F8B41B41333333333D3FF6407593188402B51B41B6F3FDD4423FF640');


INSERT INTO public.land_use(id, geometry)
VALUES (2, '0103000020D20E00000100000005000000508D976EFF97184125068195F771F240D9CEF753F6961841621058395E72F240D9CEF753F696184114AE47E1FC72F2402731082C01981841FA7E6ABCA872F240508D976EFF97184125068195F771F240');
