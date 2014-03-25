EXPLAIN SELECT 'face''book', 'face' 'book', 'face'
                                            'book',
               "face""book", "face" "book", "face"
                                            "book",
               'face' 'bo' 'ok', 'face'"book",
               "face"'book', 'facebook' FROM src LIMIT 1;

SELECT 'face''book', 'face' 'book', 'face'
                                    'book',
       "face""book", "face" "book", "face"
                                    "book",
       'face' 'bo' 'ok', 'face'"book",
       "face"'book', 'facebook' FROM src LIMIT 1;
