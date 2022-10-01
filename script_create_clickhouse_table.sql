DROP DATABASE `matomo`;

CREATE DATABASE IF NOT EXISTS `matomo`;

CREATE TABLE IF NOT EXISTS `matomo`.`log_replication` (
    `id` UInt64,
    `created_at` DateTime DEFAULT now(),
    `log_time` DateTime,
    `log_file` String,
    `log_pos_start` UInt64,
    `log_pos_end` UInt64
)
ENGINE = ReplacingMergeTree() ORDER BY (id) SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_log_visit`  (
    `idvisit` UInt64,
    `idsite` UInt32,
    `idvisitor` String,
    `visit_last_action_time` DateTime,
    `config_id` String,
    `location_ip` String,
    `profilable` Nullable(Int8),
    `user_id` Nullable(String),
    `visit_first_action_time` DateTime,
    `visit_goal_buyer` Nullable(Int8),
    `visit_goal_converted` Nullable(Int8),
    `visitor_returning` Nullable(Int8),
    `visitor_seconds_since_first` Nullable(UInt32),
    `visitor_seconds_since_order` Nullable(UInt32),
    `visitor_count_visits` UInt32,
    `visit_entry_idaction_name` Nullable(UInt32),
    `visit_entry_idaction_url` Nullable(UInt32),
    `visit_exit_idaction_name` Nullable(UInt32),
    `visit_exit_idaction_url` Nullable(UInt32),
    `visit_total_actions` Nullable(UInt32),
    `visit_total_interactions` Nullable(UInt32),
    `visit_total_searches` Nullable(UInt16),
    `referer_keyword` Nullable(String),
    `referer_name` Nullable(String),
    `referer_type` Nullable(UInt8),
    `referer_url` Nullable(String),
    `location_browser_lang` Nullable(String),
    `config_browser_engine` Nullable(String),
    `config_browser_name` Nullable(String),
    `config_browser_version` Nullable(String),
    `config_client_type` Nullable(Int8),
    `config_device_brand` Nullable(String),
    `config_device_model` Nullable(String),
    `config_device_type` Nullable(Int8),
    `config_os` Nullable(String),
    `config_os_version` Nullable(String),
    `visit_total_events` Nullable(UInt32),
    `visitor_localtime` Nullable(String),
    `visitor_seconds_since_last` Nullable(UInt32),
    `config_resolution` Nullable(String),
    `config_cookie` Nullable(Int8),
    `config_flash` Nullable(Int8),
    `config_java` Nullable(Int8),
    `config_pdf` Nullable(Int8),
    `config_quicktime` Nullable(Int8),
    `config_realplayer` Nullable(Int8),
    `config_silverlight` Nullable(Int8),
    `config_windowsmedia` Nullable(Int8),
    `visit_total_time` UInt32,
    `location_city` Nullable(String),
    `location_country` Nullable(String),
    `location_latitude` Nullable(String),
    `location_longitude` Nullable(String),
    `location_region` Nullable(String),
    `last_idlink_va` Nullable(UInt64),
    `custom_dimension_1` Nullable(String),
    `custom_dimension_2` Nullable(String),
    `custom_dimension_3` Nullable(String),
    `custom_dimension_4` Nullable(String),
    `custom_dimension_5` Nullable(String),
    `campaign_content` Nullable(String),
    `campaign_group` Nullable(String),
    `campaign_id` Nullable(String),
    `campaign_keyword` Nullable(String),
    `campaign_medium` Nullable(String),
    `campaign_name` Nullable(String),
    `campaign_placement` Nullable(String),
    `campaign_source` Nullable(String),
    `custom_var_k1` Nullable(String),
    `custom_var_v1` Nullable(String),
    `custom_var_k2` Nullable(String),
    `custom_var_v2` Nullable(String),
    `custom_var_k3` Nullable(String),
    `custom_var_v3` Nullable(String),
    `custom_var_k4` Nullable(String),
    `custom_var_v4` Nullable(String),
    `custom_var_k5` Nullable(String),
    `custom_var_v5` Nullable(String)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(visit_first_action_time) ORDER BY (idvisit);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_log_link_visit_action`  (
    `idlink_va` UInt64,
    `idsite` UInt32,
    `idvisitor` String,
    `idvisit` UInt64,
    `idaction_url_ref` Nullable(UInt32),
    `idaction_name_ref` Nullable(UInt32),
    `custom_float` Nullable(String),
    `pageview_position` Nullable(UInt32),
    `server_time` DateTime,
    `idpageview` Nullable(String),
    `idaction_name` Nullable(UInt32),
    `idaction_url` Nullable(UInt32),
    `search_cat` Nullable(String),
    `search_count` Nullable(UInt32),
    `time_spent_ref_action` Nullable(UInt32),
    `idaction_product_cat` Nullable(UInt32),
    `idaction_product_cat2` Nullable(UInt32),
    `idaction_product_cat3` Nullable(UInt32),
    `idaction_product_cat4` Nullable(UInt32),
    `idaction_product_cat5` Nullable(UInt32),
    `idaction_product_name` Nullable(UInt32),
    `product_price` Nullable(String),
    `idaction_product_sku` Nullable(UInt32),
    `idaction_event_action` Nullable(UInt32),
    `idaction_event_category` Nullable(UInt32),
    `idaction_content_interaction` Nullable(UInt32),
    `idaction_content_name` Nullable(UInt32),
    `idaction_content_piece` Nullable(UInt32),
    `idaction_content_target` Nullable(UInt32),
    `time_dom_completion` Nullable(UInt32),
    `time_dom_processing` Nullable(UInt32),
    `time_network` Nullable(UInt32),
    `time_on_load` Nullable(UInt32),
    `time_server` Nullable(UInt32),
    `time_transfer` Nullable(UInt32),
    `time_spent` Nullable(UInt32),
    `custom_dimension_1` Nullable(String),
    `custom_dimension_2` Nullable(String),
    `custom_dimension_3` Nullable(String),
    `custom_dimension_4` Nullable(String),
    `custom_dimension_5` Nullable(String),
    `bandwidth` Nullable(UInt64),
    `custom_var_k1` Nullable(String),
    `custom_var_v1` Nullable(String),
    `custom_var_k2` Nullable(String),
    `custom_var_v2` Nullable(String),
    `custom_var_k3` Nullable(String),
    `custom_var_v3` Nullable(String),
    `custom_var_k4` Nullable(String),
    `custom_var_v4` Nullable(String),
    `custom_var_k5` Nullable(String),
    `custom_var_v5` Nullable(String)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(server_time) ORDER BY (idlink_va);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_log_conversion_item`  (
    `idsite` UInt32,
    `idvisitor` String,
    `server_time` DateTime,
    `idvisit` UInt64,
    `idorder` String,
    `idaction_sku` UInt32,
    `idaction_name` UInt32,
    `idaction_category` UInt32,
    `idaction_category2` UInt32,
    `idaction_category3` UInt32,
    `idaction_category4` UInt32,
    `idaction_category5` UInt32,
    `price` String,
    `quantity` UInt32,
    `deleted` UInt8
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(server_time) ORDER BY (idvisit,idorder,idaction_sku);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_log_conversion`  (
    `idvisit` UInt64,
    `idsite` UInt32,
    `idvisitor` String,
    `server_time` DateTime,
    `idaction_url` Nullable(UInt32),
    `idlink_va` Nullable(UInt64),
    `idgoal` Int32,
    `buster` UInt32,
    `idorder` Nullable(String),
    `items` Nullable(UInt16),
    `url` String,
    `revenue` Nullable(String),
    `revenue_shipping` Nullable(String),
    `revenue_subtotal` Nullable(String),
    `revenue_tax` Nullable(String),
    `revenue_discount` Nullable(String),
    `visitor_returning` Nullable(Int8),
    `visitor_seconds_since_first` Nullable(UInt32),
    `visitor_seconds_since_order` Nullable(UInt32),
    `visitor_count_visits` UInt32,
    `referer_keyword` Nullable(String),
    `referer_name` Nullable(String),
    `referer_type` Nullable(UInt8),
    `config_browser_name` Nullable(String),
    `config_client_type` Nullable(Int8),
    `config_device_brand` Nullable(String),
    `config_device_model` Nullable(String),
    `config_device_type` Nullable(Int8),
    `location_city` Nullable(String),
    `location_country` Nullable(String),
    `location_latitude` Nullable(String),
    `location_longitude` Nullable(String),
    `location_region` Nullable(String),
    `custom_dimension_1` Nullable(String),
    `custom_dimension_2` Nullable(String),
    `custom_dimension_3` Nullable(String),
    `custom_dimension_4` Nullable(String),
    `custom_dimension_5` Nullable(String),
    `campaign_content` Nullable(String),
    `campaign_group` Nullable(String),
    `campaign_id` Nullable(String),
    `campaign_keyword` Nullable(String),
    `campaign_medium` Nullable(String),
    `campaign_name` Nullable(String),
    `campaign_placement` Nullable(String),
    `campaign_source` Nullable(String),
    `custom_var_k1` Nullable(String),
    `custom_var_v1` Nullable(String),
    `custom_var_k2` Nullable(String),
    `custom_var_v2` Nullable(String),
    `custom_var_k3` Nullable(String),
    `custom_var_v3` Nullable(String),
    `custom_var_k4` Nullable(String),
    `custom_var_v4` Nullable(String),
    `custom_var_k5` Nullable(String),
    `custom_var_v5` Nullable(String)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(server_time) ORDER BY (idvisit,idsite,idgoal,buster);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_log_profiling`  (
    `query` String,
    `count` Nullable(UInt32),
    `sum_time_ms` Nullable(String),
    `idprofiling` UInt64
) 
ENGINE = ReplacingMergeTree() ORDER BY (idprofiling);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_log_action`  (
    `idaction` UInt32,
    `name` Nullable(String),
    `hash` UInt32,
    `type` UInt8,
    `url_prefix` Nullable(Int8)
) 
ENGINE = ReplacingMergeTree() ORDER BY (idaction);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_goal`  (
    `idsite` Int32,
    `idgoal` Int32,
    `name` String,
    `description` String,
    `match_attribute` String,
    `pattern` String,
    `pattern_type` String,
    `case_sensitive` Int8,
    `allow_multiple` Int8,
    `revenue` String,
    `deleted` Int8,
    `event_value_as_revenue` Int8
) 
ENGINE = ReplacingMergeTree() ORDER BY (idsite,idgoal);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_site`  (
    `idsite` UInt32,
    `name` String,
    `main_url` String,
    `ts_created` DateTime,
    `ecommerce` Nullable(Int8),
    `sitesearch` Nullable(Int8),
    `sitesearch_keyword_parameters` String,
    `sitesearch_category_parameters` String,
    `timezone` String,
    `currency` String,
    `exclude_unknown_urls` Nullable(Int8),
    `excluded_ips` String,
    `excluded_parameters` String,
    `excluded_user_agents` String,
    `group` String,
    `type` String,
    `keep_url_fragment` Int8,
    `creator_login` Nullable(String)
) 
ENGINE = ReplacingMergeTree() ORDER BY (idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_site_url`  (
    `idsite` UInt32,
    `url` String
) 
ENGINE = ReplacingMergeTree() ORDER BY (idsite,url);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_tagmanager_tag`  (
    `idtag` UInt64,
    `idcontainerversion` UInt64,
    `idsite` UInt32,
    `type` String,
    `name` String,
    `status` String,
    `parameters` String,
    `fire_trigger_ids` String,
    `block_trigger_ids` String,
    `fire_limit` String,
    `priority` UInt16,
    `fire_delay` UInt32,
    `start_date` DateTime,
    `end_date` Nullable(DateTime),
    `created_date` DateTime,
    `updated_date` DateTime,
    `deleted_date` Nullable(DateTime)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(start_date) ORDER BY (idtag,idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_tagmanager_variable`  (
    `idvariable` UInt64,
    `idcontainerversion` UInt64,
    `idsite` UInt32,
    `type` String,
    `name` String,
    `status` String,
    `parameters` String,
    `lookup_table` String,
    `default_value` Nullable(String),
    `created_date` DateTime,
    `updated_date` DateTime,
    `deleted_date` Nullable(DateTime)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(created_date) ORDER BY (idvariable,idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_tagmanager_container_version`  (
    `idcontainerversion` UInt64,
    `idcontainer` String,
    `idsite` UInt32,
    `status` String,
    `revision` UInt32,
    `name` String,
    `description` String,
    `created_date` DateTime,
    `updated_date` DateTime,
    `deleted_date` Nullable(DateTime)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(created_date) ORDER BY (idcontainerversion,idcontainer,idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_tagmanager_container_release`  (
    `idcontainerrelease` Int64,
    `idcontainer` String,
    `idcontainerversion` UInt64,
    `idsite` UInt32,
    `status` String,
    `environment` String,
    `release_login` String,
    `release_date` DateTime,
    `deleted_date` Nullable(DateTime)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(release_date) ORDER BY (idcontainerrelease,idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_tagmanager_container`  (
    `idcontainer` String,
    `idsite` UInt32,
    `context` String,
    `name` String,
    `description` String,
    `status` String,
    `created_date` DateTime,
    `updated_date` DateTime,
    `deleted_date` Nullable(DateTime)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(created_date) ORDER BY (idcontainer,idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_tagmanager_trigger`  (
    `idtrigger` UInt64,
    `idcontainerversion` UInt64,
    `idsite` UInt32,
    `type` String,
    `name` String,
    `status` String,
    `parameters` String,
    `conditions` String,
    `created_date` DateTime,
    `updated_date` DateTime,
    `deleted_date` Nullable(DateTime)
) 
ENGINE = ReplacingMergeTree() PARTITION BY toYYYYMM(created_date) ORDER BY (idtrigger,idsite);

CREATE TABLE IF NOT EXISTS `matomo`.`matomo_custom_dimensions`  (
    `idcustomdimension` UInt64,
    `idsite` UInt64,
    `name` String,
    `index` UInt16,
    `scope` String,
    `active` UInt8,
    `extractions` String,
    `case_sensitive` UInt8
) 
ENGINE = ReplacingMergeTree() ORDER BY (idcustomdimension,idsite);
