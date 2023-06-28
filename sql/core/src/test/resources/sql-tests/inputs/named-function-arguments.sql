SELECT mask('abcd-EFGH-8765-4321', 'A', 'abc');
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd');
SELECT mask(lowerChar => 'q', upperChar => 'Q', otherChar => 'o', digitChar => 'd', str => 'AbCD123-@$#');
SELECT mask('AbCD123-@$#', lowerChar => 'q', upperChar => 'Q', digitChar => 'd');
SELECT mask(lowerChar => 'q', upperChar => 'Q', digitChar => 'd', str => 'AbCD123-@$#');
SELECT mask(lowerChar => 'q', 'AbCD123-@$#', upperChar => 'Q', otherChar => 'o', digitChar => 'd');
