-- test cases for IPv4 address functions

-- inet_aton: valid 4-segment addresses
select inet_aton('192.168.1.1');
select inet_aton('0.0.0.0');
select inet_aton('255.255.255.255');
select inet_aton('127.0.0.1');
select inet_aton('10.0.0.1');

-- inet_aton: valid short-form addresses (Flink-compatible)
select inet_aton('192.168.1');
select inet_aton('192.168');
select inet_aton('192');
select inet_aton('127.1');

-- inet_aton: leading zeros parsed as decimal
select inet_aton('010.000.000.001');

-- inet_aton: null and invalid inputs (non-ANSI)
select inet_aton(null);
select inet_aton('');
select inet_aton('invalid');
select inet_aton('256.1.1.1');
select inet_aton('-1.2.3.4');
select inet_aton('1.2.3.4.5');
select inet_aton('192..1.1');
select inet_aton('192.168.1.');
select inet_aton(' 127.0.0.1');

-- inet_ntoa: valid long values
select inet_ntoa(0L);
select inet_ntoa(4294967295L);
select inet_ntoa(3232235777L);
select inet_ntoa(2130706433L);

-- inet_ntoa: out of range (non-ANSI)
select inet_ntoa(-1L);
select inet_ntoa(4294967296L);

-- inet_ntoa: null input
select inet_ntoa(null);

-- inet_aton + inet_ntoa round-trip
select inet_ntoa(inet_aton('1.2.3.4'));
select inet_ntoa(inet_aton('10.0.0.1'));
select inet_ntoa(inet_aton('0.0.0.0'));
select inet_ntoa(inet_aton('255.255.255.255'));

-- try_inet_aton: valid input
select try_inet_aton('192.168.1.1');
select try_inet_aton('0.0.0.0');

-- try_inet_aton: invalid input always returns null
select try_inet_aton('invalid');
select try_inet_aton('');
select try_inet_aton('256.1.1.1');
select try_inet_aton(null);

-- type check
select inet_aton(1);
select inet_ntoa('abc');
