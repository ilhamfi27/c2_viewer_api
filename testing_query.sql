SELECT 
    st.system_track_number,
    mx.created_time,
    st.session_id 
FROM replay_track_general_setting st 
JOIN sessions s ON st.session_id = s.id 
JOIN 
(
    SELECT 
        session_id,
        system_track_number,
        max(created_time) created_time 
    FROM replay_track_general_setting 
    GROUP BY session_id,system_track_number
) mx ON st.system_track_number = mx.system_track_number 
and st.created_time = mx.created_time 
and st.session_id = mx.session_id 
WHERE s.end_time is NULL 
ORDER BY st.system_track_number;

INSERT INTO public.replay_track_general_setting(
	session_id, system_track_number, speed_label_visibility, track_name_label_visibility, radar_coverage_visibility, track_visibility, created_time)
	VALUES (4, 133, 'SHOW', 'SHOW', 'SHOW', 'SHOW', now());

INSERT INTO public.replay_system_track_general(
	session_id, system_track_number, track_name, network_track_number, identity, environment, object_type, object_id, primitive_data_source, accuracy_level, source, own_unit_indicator, iu_indicator, c2_indicator, special_processing_indicator, force_tell_indicator, emergency_indicator, simulation_indicator, last_update_time, initiation_time, created_time, airborne_indicator)
	VALUES (4,133,'RS0001',-1,'FRIENDLY','AIR','POINT_TRACK',1,'L',7,'DATA_LINK_TYPE','FALSE','TRUE','TRUE','FALSE','FALSE','FALSE','FALSE','2020-01-10 14:12:27.791','2020-01-10 14:12:27.791',now(),'TRUE');

1	97	'RS0097'	-1	'UNKNOWN'	'SURFACE'	'POINT_TRACK'	97	'R'	7	'RADAR_TYPE'	'FALSE'	'FALSE'	'FALSE'	'FALSE'	'FALSE'	'FALSE'	'FALSE'	'2020-01-10 14:12:27.886'	'2020-01-10 14:12:27.886'	'2020-01-10 14:12:30.179'	


DELETE FROM public.replay_track_general_setting WHERE system_track_number = 97

INSERT INTO public.replay_system_track_general(
	session_id, system_track_number, 
	track_name, network_track_number, 
	identity, environment, 
	object_type, object_id, 
	primitive_data_source, accuracy_level, 
	source, own_unit_indicator, 
	iu_indicator, c2_indicator, 
	special_processing_indicator, force_tell_indicator, 
	emergency_indicator, simulation_indicator, 
	last_update_time, initiation_time, 
	created_time, airborne_indicator)
VALUES (
	-- 15, (SELECT MAX(system_track_number) + 1 FROM replay_system_track_general WHERE session_id = 15 ), 
	15, 15, 
	'RS0285', -1, 
	'PENDING', 'SPACE', 
	'POINT_TRACK', 182, 
	'R', 7, 
	'RADAR_TYPE', 'FALSE', 
	'FALSE', 'FALSE', 
	'FALSE', 'FALSE', 
	'FALSE', 'FALSE', 
	'2020-01-10 14:16:15.561', '2020-01-10 14:16:15.561', 
	now(), 'FALSE');

INSERT INTO public.replay_system_track_kinetic(
	session_id, system_track_number, 
	track_name, heading, 
	latitude, longitude, 
	range, bearing, 
	height_depth, speed_over_ground, 
	course_over_ground, last_update_time, 
	created_time)
VALUES (
	-- 15, (SELECT MAX(system_track_number) + 1 FROM replay_system_track_kinetic WHERE session_id = 15 ), 
	15, 15, 
	'RS0285', 1.23918376891597, 
	-5.72047153442199, 100.5, 
	60416.8627231799, 4.18797088515306, 
	0, 200, 
	0.0, '2020-01-10 14:22:45.265', 
	now());

INSERT INTO public.replay_system_track_processing(
	session_id, system_track_number, 
	track_fusion_status, track_join_status, 
	daughter_tracks, track_phase_type, 
	track_suspect_level, created_time)
VALUES (
	-- 15, (SELECT MAX(system_track_number) + 1 FROM replay_system_track_processing WHERE session_id = 15 ), 
	15, 15, 
	'DATALINK_ONLY', 'NOT_JOINED_TRACK', 
	'{}', 'TRACKING', 
	'LEVEL_0', now());
    
INSERT INTO public.replay_ais_data(
	session_id, system_track_number, 
	mmsi_number, name, 
	radio_call_sign, imo_number, 
	navigation_status, destination, 
	dimensions_of_ship, type_of_ship_or_cargo, 
	rate_of_turn, position_accuracy, 
	gross_tonnage, ship_country, 
	created_time, eta_at_destination, 
	vendor_id)
VALUES (
	-- 15, (SELECT MAX(system_track_number) + 1 FROM replay_system_track_processing WHERE session_id = 16 ), 
	15, 15, 
	477995176, 'MUI WO 1', 
	'VRS4270', 9167241, 
	'UNDER_WAY_USING_ENGINE', 'HONG KONG', 
	'A=113,B=31,C=17,D=11', 'PASSENGER', 
	-2.9, 0, 
	0, 'HONG_KONG', 
	'2020-01-10 14:12:35.699', '2020-01-10 14:12:35.384', 
	'1234567');

-- INSERT INTO public.replay_track_general_setting(
-- 	session_id, system_track_number, speed_label_visibility, track_name_label_visibility, radar_coverage_visibility, track_visibility, created_time)
-- 	VALUES (15, 285, 'SHOW', 'SHOW', 'SHOW', 'SHOW', '2020-01-10 14:12:27.921');

DELETE FROM public.replay_system_track_general WHERE session_id = 15 
AND system_track_number BETWEEN 2 AND (SELECT MAX(system_track_number) FROM public.replay_system_track_general);
DELETE FROM public.replay_system_track_kinetic WHERE session_id = 15 
AND system_track_number BETWEEN 2 AND (SELECT MAX(system_track_number) FROM public.replay_system_track_kinetic);
DELETE FROM public.replay_system_track_processing WHERE session_id = 15 
AND system_track_number BETWEEN 2 AND (SELECT MAX(system_track_number) FROM public.replay_system_track_processing);

SELECT * FROM public.replay_system_track_general WHERE session_id = 15 ORDER BY 2 DESC;


INSERT INTO public.reference_points(
	object_type, object_id, 
	name, latitude, 
	longitude, altitude, 
	visibility_type, point_amplification_type, 
	is_editable, network_track_number, 
	link_status_type, last_update_time)
	VALUES ('REFERENCE_POINT', 1, 
			'point 3', -4.555, 
			102.66, 0, 
			'SHOW', 'NAVIGATION', 
			'TRUE', -1, 
			'NOT_LINK_INVOLVE', now());

INSERT INTO public.tactical_figure_list(
	object_id, object_type, 
	name, environment, 
	shape, displaying_popup_alert_status, 
	line_color, fill_color, 
	identity_list, warning_list, 
	evaluation_type, visibility_type, 
	last_update_time, network_track_number, 
	link_status_type, is_editable, 
	point_amplification_type, point_keys, 
	points
) VALUES (
	19, 'TACTICAL_FIGURE', 
	'line 4', 'AIR_SURFACE', 
	'LINE', false, 
	'{0,0,0,255}', '{255,255,255,255}', 
	'{HOSTILE,PENDING,SUSPECT,UNKNOWN}', '{ENTER_AREA,EXIT_AREA}', 
	'NO_WARNING', 'SHOW', 
	NULL, NULL,
	'2020-01-10 14:36:58.541', true, 
	'SUPPORTING_ATTACK', '{1,2}', 
	'{{-16.4974025062579,107.512915842412},{-12.85340394137489,112.508309115875}}'
);

INSERT INTO public.area_alerts(
	session_id, object_type, 
	object_id, warning_type, 
	track_name, last_update_time, 
	mmsi_number, ship_name, 
	track_source_type, is_visible)
VALUES (
	15, 'ALERT_WARNING', 
	1, 'ENTER_AREA', 
	'-S0001', now(), 
	0, '', 
	'RADAR_TYPE', 'SHOW');


21, 'TACTICAL_FIGURE', 
'line 2', 'AIR_SURFACE', 
'LINE', false, 
{0,0,0,255}, {255,255,255,255}, 
'{HOSTILE,PENDING,SUSPECT,UNKNOWN}', '{ENTER_AREA,EXIT_AREA}', 
'NO_WARNING', 'REMOVE', 
'2020-01-10 14:29:00.14', true, 
'WAY_POINT', {1,2,3,4,5},
{{1.48142350245919,125.99693980544},{-1.04590290312459,125.300371022288},{-2.23950844571119,124.484390447738},{-4.34551514538521,113.18007419544},{-7.67020424454918,114.035858700456}}

21, 'TACTICAL_FIGURE', 'line 2', 'AIR_SURFACE', 'LINE', false, '{0,0,0,255}', '{255,255,255,255}', '{HOSTILE,PENDING,SUSPECT,UNKNOWN}', '{ENTER_AREA,EXIT_AREA}', 'NO_WARNING', 'SHOW', '2020-01-10 14:29:00.14', NULL, NULL, true, 'WAY_POINT', '{1,2,3,4,5}', '{{1.48142350245919,125.99693980544}, {-1.04590290312459,125.300371022288},{-2.23950844571119,124.484390447738},{-4.34551514538521,113.18007419544},{-7.67020424454918,114.035858700456}}'
20, 'TACTICAL_FIGURE', 'line 4', 'AIR_SURFACE', 'LINE', false, '{0,0,0,255}', '{255,255,255,255}', '{HOSTILE,PENDING,SUSPECT,UNKNOWN}', '{ENTER_AREA,EXIT_AREA}', 'NO_WARNING', 'SHOW', '2020-01-10 14:36:58.541', NULL, NULL, true, 'SUPPORTING_ATTACK', '{1,2}', '{{-13.4974025062579,106.512915842412},{-9.85340394137489,114.508309115875}}'



SELECT system_track_number, latitude, longitude, 
longitude+0.1 AS longitude_2,created_time, created_time + interval '2 hour' AS create_time_2 
FROM public.replay_system_track_kinetic
WHERE session_id = 1


-- new query with history table
SELECT st.*
FROM replay_system_track_kinetic st
JOIN
sessions s ON st.session_id=s.id
JOIN his_replay_system_track_kinetic mx
ON st.system_track_number=mx.system_track_number and st.created_time=mx.max_created_date_time
and st.session_id=mx.session_id
WHERE s.end_time is NULL
ORDER BY st.system_track_number;