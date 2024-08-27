-- Выходная таблица
-- DROP TABLE public.subscribers_feedback;

CREATE TABLE public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int8 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
