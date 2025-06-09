-- --------
-- Очистка таблицы от друзей с самим собой
-- --------

delete 
from tmp_request_accepted tra 
where tra.accepter_id = tra.requester_id;

-- --------
-- Очистка таблицы от повторных значений
-- --------

delete from tmp_request_accepted 
where ctid not in (
	select distinct on (tra.requester_id, tra.accepter_id) ctid
	from tmp_request_accepted tra
	order by tra.requester_id, tra.accepter_id, tra.accept_date desc
);

-- --------
-- Очистка таблицы от обратных пар
-- --------

delete from tmp_request_accepted trad 
where trad.ctid in (
	select tra.ctid
	from tmp_request_accepted tra 
	join tmp_request_accepted tra2
		on tra.accepter_id = tra2.requester_id
		and tra.requester_id = tra2.accepter_id
	where tra.accept_date <= tra2.accept_date
);

-- --------
-- Очистка таблицы от "ошибочных" id (из errors.txt)
-- --------

delete from tmp_request_accepted tra 
where tra.requester_id in (select * from error_ids)
	or tra.accepter_id in (select * from error_ids);
