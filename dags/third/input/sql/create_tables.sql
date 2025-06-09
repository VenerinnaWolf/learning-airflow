create table request_accepted (
	requester_id   int,
	accepter_id    int,
	accept_date    date,
	primary key (requester_id, accepter_id)
);

create table tmp_request_accepted (
	requester_id   int,
	accepter_id    int,
	accept_date    date
);

create table error_ids (
	error_id int
);