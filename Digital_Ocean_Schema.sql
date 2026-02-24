--
-- PostgreSQL database dump
--

\restrict LbUtx8S9wWhMcyCoAKhvVTUkkIJCZPWphX52vRs8nQufwLkGNFP8y1lboW8c779

-- Dumped from database version 16.11
-- Dumped by pg_dump version 16.12 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: actor_type_enum; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.actor_type_enum AS ENUM (
    'contact',
    'operator',
    'system',
    'ai'
);


--
-- Name: chat_state_enum; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.chat_state_enum AS ENUM (
    'open',
    'pending',
    'closed'
);


--
-- Name: message_direction; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.message_direction AS ENUM (
    'inbound',
    'outbound'
);


--
-- Name: message_event_type_enum; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.message_event_type_enum AS ENUM (
    'created',
    'edited',
    'deleted',
    'reacted',
    'forwarded',
    'replied'
);


--
-- Name: message_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.message_status AS ENUM (
    'sent',
    'delivered',
    'read'
);


--
-- Name: message_status_enum; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.message_status_enum AS ENUM (
    'queued',
    'sent',
    'delivered',
    'read',
    'failed'
);


--
-- Name: message_type_enum; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.message_type_enum AS ENUM (
    'text',
    'image',
    'video',
    'audio',
    'document',
    'interactive',
    'reaction',
    'system'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ai_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ai_events (
    id bigint NOT NULL,
    uuid text,
    event_timestamp timestamp with time zone NOT NULL,
    event_type text NOT NULL,
    ai_vendor text,
    ai_model text,
    evaluation_context_type text,
    session_id text,
    session_external_id text,
    run_id text,
    run_external_id text,
    thread_external_id text,
    assistant_external_id text,
    prompt_tokens integer,
    completion_tokens integer,
    total_tokens integer,
    duration_ms integer,
    number_uuid text,
    assistant_uuid text,
    journey_uuid text,
    chat_uuid text,
    payload jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: ai_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ai_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ai_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ai_events_id_seq OWNED BY public.ai_events.id;


--
-- Name: ai_usage_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ai_usage_daily (
    usage_date date NOT NULL,
    model text NOT NULL,
    total_requests integer,
    total_tokens integer,
    avg_latency_ms integer,
    failures integer
);


--
-- Name: audit_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_logs (
    id bigint NOT NULL,
    actor_type public.actor_type_enum NOT NULL,
    actor_id uuid,
    action text NOT NULL,
    entity_type text,
    entity_id text,
    payload jsonb,
    occurred_at timestamp with time zone DEFAULT now()
);


--
-- Name: audit_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.audit_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: audit_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.audit_logs_id_seq OWNED BY public.audit_logs.id;


--
-- Name: campaign_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.campaign_metrics (
    campaign_id uuid NOT NULL,
    sent_count integer DEFAULT 0,
    delivered_count integer DEFAULT 0,
    read_count integer DEFAULT 0,
    reply_count integer DEFAULT 0,
    conversion_count integer DEFAULT 0,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: campaign_metrics_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.campaign_metrics_daily (
    campaign_id uuid NOT NULL,
    metric_date date NOT NULL,
    sent_count integer DEFAULT 0,
    delivered_count integer DEFAULT 0,
    read_count integer DEFAULT 0,
    reply_count integer DEFAULT 0,
    conversion_count integer DEFAULT 0
);


--
-- Name: campaigns; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.campaigns (
    id uuid NOT NULL,
    name text NOT NULL,
    type text,
    created_at timestamp with time zone DEFAULT now(),
    is_deleted boolean DEFAULT false
);


--
-- Name: chat_assignments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_assignments (
    id bigint NOT NULL,
    chat_id bigint NOT NULL,
    operator_id uuid,
    assigned_at timestamp with time zone DEFAULT now() NOT NULL,
    unassigned_at timestamp with time zone
);


--
-- Name: chat_assignments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.chat_assignments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: chat_assignments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.chat_assignments_id_seq OWNED BY public.chat_assignments.id;


--
-- Name: chat_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chat_state (
    chat_id bigint NOT NULL,
    last_message_at timestamp with time zone,
    last_inbound_at timestamp with time zone,
    last_outbound_at timestamp with time zone,
    unread_count integer,
    current_status text,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: chats_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.chats_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: chats; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chats (
    id bigint DEFAULT nextval('public.chats_id_seq'::regclass) NOT NULL,
    uuid uuid,
    number_id bigint NOT NULL,
    contact_id bigint,
    state text,
    state_reason text,
    type text,
    title text,
    owner text,
    icon text,
    invite_link text,
    unread_count integer DEFAULT 0,
    is_culled boolean DEFAULT false,
    is_blocked boolean DEFAULT false,
    needs_help boolean DEFAULT false,
    should_count_for_non_blocked_stats boolean DEFAULT true,
    should_count_for_blocked_stats boolean DEFAULT false,
    last_message_id bigint,
    last_reaction_uuid uuid,
    inbound_timestamp timestamp with time zone,
    outbound_timestamp timestamp with time zone,
    pinned_at timestamp with time zone,
    assigned_to_timestamp timestamp with time zone,
    assigned_to_uuid uuid,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    is_deleted boolean DEFAULT false
);


--
-- Name: contact_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contact_metrics (
    contact_id bigint NOT NULL,
    first_seen_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    total_messages_received integer DEFAULT 0,
    total_messages_sent integer DEFAULT 0,
    last_message_at timestamp with time zone,
    engagement_score integer,
    engagement_level text,
    onboarded boolean,
    opted_in boolean,
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT chk_engagement_level CHECK ((engagement_level = ANY (ARRAY['low'::text, 'medium'::text, 'high'::text])))
);


--
-- Name: contacts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.contacts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: contacts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contacts (
    id bigint DEFAULT nextval('public.contacts_id_seq'::regclass) NOT NULL,
    uuid uuid,
    number_id bigint NOT NULL,
    whatsapp_id text,
    whatsapp_profile_name text,
    first_name text,
    last_name text,
    age text,
    city text,
    location text,
    language text,
    business_category text,
    business_type text,
    business_age text,
    onboarded boolean DEFAULT false,
    opted_in boolean DEFAULT false,
    opted_in_at timestamp with time zone,
    first_message_received_at timestamp with time zone,
    last_message_received_at timestamp with time zone,
    last_message_sent_at timestamp with time zone,
    last_seen_at timestamp with time zone,
    failure_count integer DEFAULT 0,
    is_deleted boolean DEFAULT false,
    is_fallback_active boolean DEFAULT false,
    last_update_origin text,
    details jsonb,
    external_id text,
    type text,
    urn text,
    organisation_id text,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: conversation_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conversation_sessions (
    id uuid NOT NULL,
    contact_id bigint,
    chat_id bigint,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    source text,
    is_ai boolean,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: daily_metrics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.daily_metrics (
    metric_date date NOT NULL,
    dau integer,
    wau integer,
    mau integer,
    messages_sent integer,
    messages_read integer,
    new_contacts integer,
    churned_contacts integer
);


--
-- Name: dashboard_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_cache (
    cache_key text NOT NULL,
    payload jsonb,
    expires_at timestamp with time zone
);


--
-- Name: ingestion_scheduler_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ingestion_scheduler_state (
    id integer NOT NULL,
    last_recovery_date date,
    last_audit_date date,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: ingestion_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ingestion_state (
    id integer NOT NULL,
    last_external_timestamp timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: journey_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.journey_events (
    contact_id bigint,
    journey_id text,
    step_id text,
    event_type text,
    occurred_at timestamp with time zone,
    campaign_id uuid
);


--
-- Name: journey_progress; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.journey_progress (
    contact_id bigint NOT NULL,
    journey_id text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    current_step text,
    completion_percentage integer,
    campaign_id uuid
);


--
-- Name: journeys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.journeys (
    id text NOT NULL,
    name text,
    version integer,
    is_active boolean,
    created_at timestamp with time zone,
    is_deleted boolean DEFAULT false
);


--
-- Name: message_attachments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.message_attachments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: message_attachments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.message_attachments (
    id bigint DEFAULT nextval('public.message_attachments_id_seq'::regclass) NOT NULL,
    message_id bigint NOT NULL,
    chat_id bigint NOT NULL,
    number_id bigint NOT NULL,
    attachment_id bigint,
    filename text,
    caption text,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: message_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.message_events (
    id bigint NOT NULL,
    message_id bigint NOT NULL,
    event_type public.message_event_type_enum NOT NULL,
    actor_type public.actor_type_enum NOT NULL,
    actor_id uuid,
    payload jsonb,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: message_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.message_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: message_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.message_events_id_seq OWNED BY public.message_events.id;


--
-- Name: message_metrics_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.message_metrics_daily (
    metric_date date NOT NULL,
    number_id bigint NOT NULL,
    campaign_id uuid NOT NULL,
    direction public.message_direction NOT NULL,
    message_count integer
);


--
-- Name: message_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.message_tags (
    id bigint NOT NULL,
    message_id bigint NOT NULL,
    number_tag_id bigint NOT NULL,
    number_id bigint NOT NULL,
    confidence real DEFAULT 1.0,
    metadata jsonb,
    enabled boolean DEFAULT true,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: message_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.message_tags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: message_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.message_tags_id_seq OWNED BY public.message_tags.id;


--
-- Name: messages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.messages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.messages (
    id bigint DEFAULT nextval('public.messages_id_seq'::regclass) NOT NULL,
    uuid uuid,
    number_id bigint,
    chat_id bigint,
    message_type public.message_type_enum,
    author text,
    author_type text,
    from_addr text,
    content text,
    rendered_content text,
    message_metadata jsonb,
    media_object jsonb,
    flow_response_json jsonb,
    raw_body jsonb,
    has_media boolean DEFAULT false,
    is_deleted boolean DEFAULT false,
    is_handled boolean DEFAULT false,
    last_status text,
    last_status_timestamp timestamp with time zone,
    external_id text,
    external_timestamp timestamp with time zone,
    card_uuid uuid,
    faq_uuid uuid,
    opt_in_uuid uuid,
    reaction text,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    campaign_id uuid,
    direction public.message_direction NOT NULL
);


--
-- Name: mv_recent_chats; Type: MATERIALIZED VIEW; Schema: public; Owner: -
--

CREATE MATERIALIZED VIEW public.mv_recent_chats AS
 SELECT c.id AS chat_id,
    c.contact_id,
    c.state,
    c.updated_at,
    m.content AS last_message
   FROM (public.chats c
     LEFT JOIN LATERAL ( SELECT messages.content
           FROM public.messages
          WHERE (messages.chat_id = c.id)
          ORDER BY messages.inserted_at DESC
         LIMIT 1) m ON (true))
  WITH NO DATA;


--
-- Name: number_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.number_tags (
    id bigint NOT NULL,
    uuid text,
    value text NOT NULL,
    color text,
    number_id bigint NOT NULL,
    enabled boolean DEFAULT true,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: number_tags_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.number_tags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: number_tags_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.number_tags_id_seq OWNED BY public.number_tags.id;


--
-- Name: numbers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.numbers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: numbers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.numbers (
    id bigint DEFAULT nextval('public.numbers_id_seq'::regclass) NOT NULL,
    provider text NOT NULL,
    phone_number text NOT NULL,
    display_name text,
    country text,
    timezone text,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: operator_metrics_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.operator_metrics_daily (
    operator_id uuid NOT NULL,
    metric_date date NOT NULL,
    messages_handled integer DEFAULT 0,
    chats_closed integer DEFAULT 0,
    avg_response_time_ms integer
);


--
-- Name: operators; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.operators (
    id uuid NOT NULL,
    name text,
    email text,
    role text,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    is_deleted boolean DEFAULT false
);


--
-- Name: statuses; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.statuses (
    id bigint NOT NULL,
    message_id bigint NOT NULL,
    message_uuid text,
    number_id bigint NOT NULL,
    status public.message_status_enum NOT NULL,
    description text,
    raw_body jsonb,
    errors jsonb,
    status_timestamp timestamp with time zone,
    inserted_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: statuses_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.statuses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: statuses_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.statuses_id_seq OWNED BY public.statuses.id;


--
-- Name: webhook_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webhook_events (
    id bigint NOT NULL,
    provider text,
    event_type text,
    external_event_id text,
    payload jsonb,
    received_at timestamp with time zone DEFAULT now(),
    processed boolean DEFAULT false
);


--
-- Name: webhook_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.webhook_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: webhook_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.webhook_events_id_seq OWNED BY public.webhook_events.id;


--
-- Name: webhook_failures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.webhook_failures (
    id bigint NOT NULL,
    webhook_event_id bigint,
    error_message text,
    retry_count integer DEFAULT 0,
    last_retry_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    terminal_failure boolean DEFAULT false
);


--
-- Name: webhook_failures_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.webhook_failures_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: webhook_failures_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.webhook_failures_id_seq OWNED BY public.webhook_failures.id;


--
-- Name: ai_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ai_events ALTER COLUMN id SET DEFAULT nextval('public.ai_events_id_seq'::regclass);


--
-- Name: audit_logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_logs ALTER COLUMN id SET DEFAULT nextval('public.audit_logs_id_seq'::regclass);


--
-- Name: chat_assignments id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_assignments ALTER COLUMN id SET DEFAULT nextval('public.chat_assignments_id_seq'::regclass);


--
-- Name: message_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_events ALTER COLUMN id SET DEFAULT nextval('public.message_events_id_seq'::regclass);


--
-- Name: message_tags id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_tags ALTER COLUMN id SET DEFAULT nextval('public.message_tags_id_seq'::regclass);


--
-- Name: number_tags id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.number_tags ALTER COLUMN id SET DEFAULT nextval('public.number_tags_id_seq'::regclass);


--
-- Name: statuses id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.statuses ALTER COLUMN id SET DEFAULT nextval('public.statuses_id_seq'::regclass);


--
-- Name: webhook_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_events ALTER COLUMN id SET DEFAULT nextval('public.webhook_events_id_seq'::regclass);


--
-- Name: webhook_failures id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_failures ALTER COLUMN id SET DEFAULT nextval('public.webhook_failures_id_seq'::regclass);


--
-- Name: ai_events ai_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ai_events
    ADD CONSTRAINT ai_events_pkey PRIMARY KEY (id);


--
-- Name: ai_events ai_events_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ai_events
    ADD CONSTRAINT ai_events_uuid_key UNIQUE (uuid);


--
-- Name: ai_usage_daily ai_usage_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ai_usage_daily
    ADD CONSTRAINT ai_usage_daily_pkey PRIMARY KEY (usage_date, model);


--
-- Name: audit_logs audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_logs
    ADD CONSTRAINT audit_logs_pkey PRIMARY KEY (id);


--
-- Name: campaign_metrics_daily campaign_metrics_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.campaign_metrics_daily
    ADD CONSTRAINT campaign_metrics_daily_pkey PRIMARY KEY (campaign_id, metric_date);


--
-- Name: campaign_metrics campaign_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.campaign_metrics
    ADD CONSTRAINT campaign_metrics_pkey PRIMARY KEY (campaign_id);


--
-- Name: campaigns campaigns_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.campaigns
    ADD CONSTRAINT campaigns_pkey PRIMARY KEY (id);


--
-- Name: chat_assignments chat_assignments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_assignments
    ADD CONSTRAINT chat_assignments_pkey PRIMARY KEY (id);


--
-- Name: chat_state chat_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_state
    ADD CONSTRAINT chat_state_pkey PRIMARY KEY (chat_id);


--
-- Name: chats chats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chats
    ADD CONSTRAINT chats_pkey PRIMARY KEY (id);


--
-- Name: chats chats_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chats
    ADD CONSTRAINT chats_uuid_key UNIQUE (uuid);


--
-- Name: contact_metrics contact_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contact_metrics
    ADD CONSTRAINT contact_metrics_pkey PRIMARY KEY (contact_id);


--
-- Name: contacts contacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contacts
    ADD CONSTRAINT contacts_pkey PRIMARY KEY (id);


--
-- Name: contacts contacts_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contacts
    ADD CONSTRAINT contacts_uuid_key UNIQUE (uuid);


--
-- Name: conversation_sessions conversation_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_sessions
    ADD CONSTRAINT conversation_sessions_pkey PRIMARY KEY (id);


--
-- Name: daily_metrics daily_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_metrics
    ADD CONSTRAINT daily_metrics_pkey PRIMARY KEY (metric_date);


--
-- Name: dashboard_cache dashboard_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_cache
    ADD CONSTRAINT dashboard_cache_pkey PRIMARY KEY (cache_key);


--
-- Name: ingestion_scheduler_state ingestion_scheduler_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ingestion_scheduler_state
    ADD CONSTRAINT ingestion_scheduler_state_pkey PRIMARY KEY (id);


--
-- Name: ingestion_state ingestion_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ingestion_state
    ADD CONSTRAINT ingestion_state_pkey PRIMARY KEY (id);


--
-- Name: journey_progress journey_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journey_progress
    ADD CONSTRAINT journey_progress_pkey PRIMARY KEY (contact_id, journey_id);


--
-- Name: journeys journeys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journeys
    ADD CONSTRAINT journeys_pkey PRIMARY KEY (id);


--
-- Name: message_attachments message_attachments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT message_attachments_pkey PRIMARY KEY (id);


--
-- Name: message_events message_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_events
    ADD CONSTRAINT message_events_pkey PRIMARY KEY (id);


--
-- Name: message_metrics_daily message_metrics_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_metrics_daily
    ADD CONSTRAINT message_metrics_daily_pkey PRIMARY KEY (metric_date, number_id, campaign_id, direction);


--
-- Name: message_tags message_tags_message_id_number_tag_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_tags
    ADD CONSTRAINT message_tags_message_id_number_tag_id_key UNIQUE (message_id, number_tag_id);


--
-- Name: message_tags message_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_tags
    ADD CONSTRAINT message_tags_pkey PRIMARY KEY (id);


--
-- Name: messages messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_pkey PRIMARY KEY (id);


--
-- Name: number_tags number_tags_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.number_tags
    ADD CONSTRAINT number_tags_pkey PRIMARY KEY (id);


--
-- Name: number_tags number_tags_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.number_tags
    ADD CONSTRAINT number_tags_uuid_key UNIQUE (uuid);


--
-- Name: numbers numbers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.numbers
    ADD CONSTRAINT numbers_pkey PRIMARY KEY (id);


--
-- Name: operator_metrics_daily operator_metrics_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.operator_metrics_daily
    ADD CONSTRAINT operator_metrics_daily_pkey PRIMARY KEY (operator_id, metric_date);


--
-- Name: operators operators_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.operators
    ADD CONSTRAINT operators_pkey PRIMARY KEY (id);


--
-- Name: statuses statuses_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.statuses
    ADD CONSTRAINT statuses_pkey PRIMARY KEY (id);


--
-- Name: ai_usage_daily uq_ai_usage_daily; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ai_usage_daily
    ADD CONSTRAINT uq_ai_usage_daily UNIQUE (usage_date, model);


--
-- Name: contact_metrics uq_contact_metrics; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contact_metrics
    ADD CONSTRAINT uq_contact_metrics UNIQUE (contact_id);


--
-- Name: daily_metrics uq_daily_metrics; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_metrics
    ADD CONSTRAINT uq_daily_metrics UNIQUE (metric_date);


--
-- Name: message_tags uq_message_tag; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_tags
    ADD CONSTRAINT uq_message_tag UNIQUE (message_id, number_tag_id);


--
-- Name: ai_usage_daily ux_ai_usage_daily; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ai_usage_daily
    ADD CONSTRAINT ux_ai_usage_daily UNIQUE (usage_date, model);


--
-- Name: daily_metrics ux_daily_metrics; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_metrics
    ADD CONSTRAINT ux_daily_metrics UNIQUE (metric_date);


--
-- Name: webhook_events webhook_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_events
    ADD CONSTRAINT webhook_events_pkey PRIMARY KEY (id);


--
-- Name: webhook_failures webhook_failures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_failures
    ADD CONSTRAINT webhook_failures_pkey PRIMARY KEY (id);


--
-- Name: webhook_failures webhook_failures_webhook_event_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_failures
    ADD CONSTRAINT webhook_failures_webhook_event_id_key UNIQUE (webhook_event_id);


--
-- Name: chats_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX chats_updated_at_idx ON public.chats USING btree (updated_at DESC);


--
-- Name: contact_metrics_engagement_level_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX contact_metrics_engagement_level_idx ON public.contact_metrics USING btree (engagement_level);


--
-- Name: daily_metrics_metric_date_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX daily_metrics_metric_date_idx ON public.daily_metrics USING btree (metric_date);


--
-- Name: idx_ai_events_chat_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_chat_uuid ON public.ai_events USING btree (chat_uuid);


--
-- Name: idx_ai_events_event_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_event_timestamp ON public.ai_events USING btree (event_timestamp DESC);


--
-- Name: idx_ai_events_event_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_event_type ON public.ai_events USING btree (event_type);


--
-- Name: idx_ai_events_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_model ON public.ai_events USING btree (ai_model);


--
-- Name: idx_ai_events_payload_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_payload_gin ON public.ai_events USING gin (payload jsonb_path_ops);


--
-- Name: idx_ai_events_run_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_run_external_id ON public.ai_events USING btree (run_external_id);


--
-- Name: idx_ai_events_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_time ON public.ai_events USING btree (event_timestamp);


--
-- Name: idx_ai_events_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_timestamp ON public.ai_events USING btree (event_timestamp);


--
-- Name: idx_ai_events_tokens; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ai_events_tokens ON public.ai_events USING btree (total_tokens);


--
-- Name: idx_chat_assignments_chat_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chat_assignments_chat_id ON public.chat_assignments USING btree (chat_id);


--
-- Name: idx_chats_contact; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chats_contact ON public.chats USING btree (contact_id);


--
-- Name: idx_chats_contact_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chats_contact_id ON public.chats USING btree (contact_id);


--
-- Name: idx_chats_inserted_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chats_inserted_at ON public.chats USING btree (inserted_at);


--
-- Name: idx_chats_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chats_number_id ON public.chats USING btree (number_id);


--
-- Name: idx_chats_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_chats_state ON public.chats USING btree (state);


--
-- Name: idx_contact_metrics_engagement; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_contact_metrics_engagement ON public.contact_metrics USING btree (engagement_level);


--
-- Name: idx_contacts_inserted_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_contacts_inserted_at ON public.contacts USING btree (inserted_at);


--
-- Name: idx_contacts_last_seen; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_contacts_last_seen ON public.contacts USING btree (last_seen_at);


--
-- Name: idx_contacts_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_contacts_number_id ON public.contacts USING btree (number_id);


--
-- Name: idx_contacts_onboarded; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_contacts_onboarded ON public.contacts USING btree (onboarded);


--
-- Name: idx_contacts_whatsapp_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_contacts_whatsapp_id ON public.contacts USING btree (whatsapp_id);


--
-- Name: idx_conversation_sessions_contact; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_conversation_sessions_contact ON public.conversation_sessions USING btree (contact_id, started_at DESC);


--
-- Name: idx_daily_metrics_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_metrics_date ON public.daily_metrics USING btree (metric_date);


--
-- Name: idx_journey_events_contact; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_journey_events_contact ON public.journey_events USING btree (contact_id, occurred_at);


--
-- Name: idx_journey_events_contact_journey; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_journey_events_contact_journey ON public.journey_events USING btree (contact_id, journey_id);


--
-- Name: idx_message_attachments_chat_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_attachments_chat_id ON public.message_attachments USING btree (chat_id);


--
-- Name: idx_message_attachments_message_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_attachments_message_id ON public.message_attachments USING btree (message_id);


--
-- Name: idx_message_attachments_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_attachments_number_id ON public.message_attachments USING btree (number_id);


--
-- Name: idx_message_events_message_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_events_message_id ON public.message_events USING btree (message_id);


--
-- Name: idx_message_events_msg_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_events_msg_type ON public.message_events USING btree (message_id, event_type);


--
-- Name: idx_message_events_occurred_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_events_occurred_at ON public.message_events USING btree (occurred_at);


--
-- Name: idx_message_tags_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_tags_enabled ON public.message_tags USING btree (enabled);


--
-- Name: idx_message_tags_message_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_tags_message_id ON public.message_tags USING btree (message_id);


--
-- Name: idx_message_tags_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_tags_number_id ON public.message_tags USING btree (number_id);


--
-- Name: idx_message_tags_number_tag_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_message_tags_number_tag_id ON public.message_tags USING btree (number_tag_id);


--
-- Name: idx_messages_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_active ON public.messages USING btree (chat_id, inserted_at) WHERE (is_deleted = false);


--
-- Name: idx_messages_chat_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_chat_id ON public.messages USING btree (chat_id);


--
-- Name: idx_messages_chat_id_inserted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_chat_id_inserted ON public.messages USING btree (chat_id, inserted_at);


--
-- Name: idx_messages_chat_inserted; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_chat_inserted ON public.messages USING btree (chat_id, inserted_at);


--
-- Name: idx_messages_chat_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_chat_time ON public.messages USING btree (chat_id, inserted_at DESC);


--
-- Name: idx_messages_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_external_id ON public.messages USING btree (external_id);


--
-- Name: idx_messages_external_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_external_time ON public.messages USING btree (external_timestamp);


--
-- Name: idx_messages_inserted_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_inserted_at ON public.messages USING btree (inserted_at);


--
-- Name: idx_messages_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_messages_number_id ON public.messages USING btree (number_id);


--
-- Name: idx_number_tags_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_number_tags_enabled ON public.number_tags USING btree (enabled);


--
-- Name: idx_number_tags_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_number_tags_number_id ON public.number_tags USING btree (number_id);


--
-- Name: idx_number_tags_value; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_number_tags_value ON public.number_tags USING btree (value);


--
-- Name: idx_statuses_message_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_message_id ON public.statuses USING btree (message_id);


--
-- Name: idx_statuses_message_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_message_status ON public.statuses USING btree (message_id, status);


--
-- Name: idx_statuses_message_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_message_time ON public.statuses USING btree (message_id, status_timestamp DESC);


--
-- Name: idx_statuses_message_uuid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_message_uuid ON public.statuses USING btree (message_uuid);


--
-- Name: idx_statuses_number_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_number_id ON public.statuses USING btree (number_id);


--
-- Name: idx_statuses_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_status ON public.statuses USING btree (status);


--
-- Name: idx_statuses_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_statuses_timestamp ON public.statuses USING btree (status_timestamp);


--
-- Name: idx_webhook_events_unprocessed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_webhook_events_unprocessed ON public.webhook_events USING btree (processed, id) WHERE (processed = false);


--
-- Name: idx_webhook_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_webhook_unique ON public.webhook_events USING btree (provider, external_event_id);


--
-- Name: messages_chat_id_inserted_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX messages_chat_id_inserted_at_idx ON public.messages USING btree (chat_id, inserted_at DESC);


--
-- Name: statuses_message_id_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX statuses_message_id_status_idx ON public.statuses USING btree (message_id, status);


--
-- Name: ux_chats_number_contact; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_chats_number_contact ON public.chats USING btree (number_id, contact_id);


--
-- Name: ux_contacts_number_whatsapp; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_contacts_number_whatsapp ON public.contacts USING btree (number_id, whatsapp_id);


--
-- Name: ux_message_attachments_message; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_message_attachments_message ON public.message_attachments USING btree (message_id);


--
-- Name: ux_message_events_msg_created; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_message_events_msg_created ON public.message_events USING btree (message_id, event_type) WHERE (event_type = 'created'::public.message_event_type_enum);


--
-- Name: ux_messages_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_messages_external_id ON public.messages USING btree (external_id);


--
-- Name: ux_number_tags_number_value; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_number_tags_number_value ON public.number_tags USING btree (number_id, value);


--
-- Name: ux_numbers_phone; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_numbers_phone ON public.numbers USING btree (phone_number);


--
-- Name: ux_statuses_message_status; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_statuses_message_status ON public.statuses USING btree (message_id, status);


--
-- Name: ux_webhook_event; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_webhook_event ON public.webhook_events USING btree (provider, external_event_id);


--
-- Name: ux_webhook_provider_event; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_webhook_provider_event ON public.webhook_events USING btree (provider, external_event_id);


--
-- Name: campaign_metrics campaign_metrics_campaign_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.campaign_metrics
    ADD CONSTRAINT campaign_metrics_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES public.campaigns(id) ON DELETE CASCADE;


--
-- Name: campaign_metrics_daily campaign_metrics_daily_campaign_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.campaign_metrics_daily
    ADD CONSTRAINT campaign_metrics_daily_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES public.campaigns(id) ON DELETE CASCADE;


--
-- Name: chat_assignments chat_assignments_chat_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_assignments
    ADD CONSTRAINT chat_assignments_chat_id_fkey FOREIGN KEY (chat_id) REFERENCES public.chats(id) ON DELETE CASCADE;


--
-- Name: chat_assignments chat_assignments_operator_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_assignments
    ADD CONSTRAINT chat_assignments_operator_id_fkey FOREIGN KEY (operator_id) REFERENCES public.operators(id);


--
-- Name: chat_state chat_state_chat_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_state
    ADD CONSTRAINT chat_state_chat_id_fkey FOREIGN KEY (chat_id) REFERENCES public.chats(id);


--
-- Name: contact_metrics contact_metrics_contact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contact_metrics
    ADD CONSTRAINT contact_metrics_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES public.contacts(id);


--
-- Name: conversation_sessions conversation_sessions_chat_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_sessions
    ADD CONSTRAINT conversation_sessions_chat_id_fkey FOREIGN KEY (chat_id) REFERENCES public.chats(id);


--
-- Name: conversation_sessions conversation_sessions_contact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_sessions
    ADD CONSTRAINT conversation_sessions_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES public.contacts(id);


--
-- Name: message_attachments fk_attachment_chat; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT fk_attachment_chat FOREIGN KEY (chat_id) REFERENCES public.chats(id) ON DELETE CASCADE;


--
-- Name: message_attachments fk_attachment_message; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_attachments
    ADD CONSTRAINT fk_attachment_message FOREIGN KEY (message_id) REFERENCES public.messages(id) ON DELETE CASCADE;


--
-- Name: chat_state fk_chat_state_chat; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chat_state
    ADD CONSTRAINT fk_chat_state_chat FOREIGN KEY (chat_id) REFERENCES public.chats(id);


--
-- Name: chats fk_chats_contact; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chats
    ADD CONSTRAINT fk_chats_contact FOREIGN KEY (contact_id) REFERENCES public.contacts(id) ON DELETE SET NULL;


--
-- Name: chats fk_chats_operator; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chats
    ADD CONSTRAINT fk_chats_operator FOREIGN KEY (assigned_to_uuid) REFERENCES public.operators(id);


--
-- Name: journey_progress fk_journey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journey_progress
    ADD CONSTRAINT fk_journey FOREIGN KEY (journey_id) REFERENCES public.journeys(id);


--
-- Name: message_tags fk_message_tags_message; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_tags
    ADD CONSTRAINT fk_message_tags_message FOREIGN KEY (message_id) REFERENCES public.messages(id) ON DELETE CASCADE;


--
-- Name: message_tags fk_message_tags_number_tag; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_tags
    ADD CONSTRAINT fk_message_tags_number_tag FOREIGN KEY (number_tag_id) REFERENCES public.number_tags(id) ON DELETE CASCADE;


--
-- Name: messages fk_messages_chat; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT fk_messages_chat FOREIGN KEY (chat_id) REFERENCES public.chats(id) ON DELETE CASCADE;


--
-- Name: messages fk_messages_contact; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT fk_messages_contact FOREIGN KEY (chat_id) REFERENCES public.chats(id);


--
-- Name: statuses fk_status_message; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.statuses
    ADD CONSTRAINT fk_status_message FOREIGN KEY (message_id) REFERENCES public.messages(id) ON DELETE CASCADE;


--
-- Name: journey_events journey_events_campaign_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journey_events
    ADD CONSTRAINT journey_events_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES public.campaigns(id);


--
-- Name: journey_events journey_events_contact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journey_events
    ADD CONSTRAINT journey_events_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES public.contacts(id);


--
-- Name: journey_progress journey_progress_campaign_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journey_progress
    ADD CONSTRAINT journey_progress_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES public.campaigns(id);


--
-- Name: journey_progress journey_progress_contact_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journey_progress
    ADD CONSTRAINT journey_progress_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES public.contacts(id);


--
-- Name: message_events message_events_message_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_events
    ADD CONSTRAINT message_events_message_id_fkey FOREIGN KEY (message_id) REFERENCES public.messages(id) ON DELETE CASCADE;


--
-- Name: messages messages_campaign_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.messages
    ADD CONSTRAINT messages_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES public.campaigns(id);


--
-- Name: operator_metrics_daily operator_metrics_daily_operator_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.operator_metrics_daily
    ADD CONSTRAINT operator_metrics_daily_operator_id_fkey FOREIGN KEY (operator_id) REFERENCES public.operators(id) ON DELETE CASCADE;


--
-- Name: webhook_failures webhook_failures_webhook_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.webhook_failures
    ADD CONSTRAINT webhook_failures_webhook_event_id_fkey FOREIGN KEY (webhook_event_id) REFERENCES public.webhook_events(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict LbUtx8S9wWhMcyCoAKhvVTUkkIJCZPWphX52vRs8nQufwLkGNFP8y1lboW8c779

