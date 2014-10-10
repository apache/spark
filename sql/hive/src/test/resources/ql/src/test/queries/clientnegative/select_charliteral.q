-- Check that charSetLiteral syntax conformance
-- Check that a sane error message with correct line/column numbers is emitted with helpful context tokens.
select _c17, count(1) from tmp_tl_foo group by _c17
