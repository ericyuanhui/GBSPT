CREATE NODE TABLE `User` (
  `id` STRING,
  `account_id` STRING,
  `usergroup_id` STRING,
  `groupinfo_id` STRING,
  `usertemplate_id` STRING,
  `visitor_auth_type` INT64,
  PRIMARY KEY(`id`)
);

CREATE NODE TABLE `Session` (
  `id` STRING,
  `login_ts` INT64,
  `logout_ts` INT64,
  `online_sec` INT64,
  `access_type` INT64,
  `term_cause` STRING,
  `time_seg` STRING,
  `total_traffic` DOUBLE,
  `gw_strategy` STRING,
  `actual_gw_strategy` STRING,
  `proxy_name` STRING,
  `policy_id` STRING,
  `accounting_rule` STRING,
  `package_name` STRING,
  `service_suffix` STRING,
  `service_id` STRING,
  `is_roaming` BOOL,
  `passthrough_type` STRING,
  PRIMARY KEY(`id`)
);

CREATE NODE TABLE `NASDevice` (
  `id` STRING,
  `name` STRING,
  `type` STRING,
  `location` STRING,
  `ipv6` STRING,
  `port_id` STRING,
  PRIMARY KEY(`id`)
);

CREATE NODE TABLE `Domain` (
  `id` STRING,
  PRIMARY KEY(`id`)
);

CREATE REL TABLE `User_owns_Session` (
  FROM `User` TO `Session`,
  `login_ts` INT64,
  `logout_ts` INT64,
  `access_type` INT64,
  MANY_MANY
);

CREATE REL TABLE `Session_via_NAS` (
  FROM `Session` TO `NASDevice`,
  `nas_port` INT64,
  MANY_ONE
);

CREATE REL TABLE `User_belongsTo_Domain` (
  FROM `User` TO `Domain`,
  MANY_ONE
);

