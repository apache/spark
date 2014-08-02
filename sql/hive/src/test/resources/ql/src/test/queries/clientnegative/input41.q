select * from 
  (select * from src 
    union all
   select * from srcpart where ds = '2009-08-09'
  )x;
