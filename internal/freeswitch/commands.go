package freeswitch

import (
	"fmt"
	"strings"
)

func (es *EventSocket) StartRecording(uuid, filename string) error {
	es.SendAPI(fmt.Sprintf("uuid_setvar %s record_read_only true", uuid))
	es.SendAPI(fmt.Sprintf("uuid_setvar %s record_stereo false", uuid))
	es.SendAPI(fmt.Sprintf("uuid_setvar %s enable_file_write_buffering false", uuid))
	es.SendAPI(fmt.Sprintf("uuid_setvar %s timer_name soft", uuid))
	_, err := es.SendAPI(fmt.Sprintf("uuid_record %s start %s 8000 1", uuid, filename))
	return err
}

func (es *EventSocket) StartWAVRecording(uuid, filename string) error {
	_, err := es.SendAPI(fmt.Sprintf("uuid_record %s start %s", uuid, filename))
	return err
}

func (es *EventSocket) StartStereoRecording(uuid, filename string) error {
	es.SendAPI(fmt.Sprintf("uuid_setvar %s record_read_only false", uuid))
	es.SendAPI(fmt.Sprintf("uuid_setvar %s RECORD_STEREO true", uuid))
	_, err := es.SendAPI(fmt.Sprintf("uuid_record %s start %s", uuid, filename))
	return err
}

func (es *EventSocket) StopRecording(uuid, filename string) error {
	_, err := es.SendAPI(fmt.Sprintf("uuid_record %s stop %s", uuid, filename))
	return err
}

// Answer issues uuid_answer for a parked/ringing channel. Idempotent on
// already-answered channels — FS returns +OK without side effects.
func (es *EventSocket) Answer(uuid string) error {
	_, err := es.SendAPI(fmt.Sprintf("uuid_answer %s", uuid))
	return err
}

func (es *EventSocket) PlayAudio(uuid, audioFile string) error {
	_, err := es.SendAPI(fmt.Sprintf("uuid_broadcast %s %s", uuid, audioFile))
	return err
}

func (es *EventSocket) StopPlayback(uuid string) error {
	_, err := es.SendAPI(fmt.Sprintf("uuid_break %s all", uuid))
	return err
}

func (es *EventSocket) EndCall(uuid string) error {
	_, err := es.SendAPI(fmt.Sprintf("uuid_kill %s", uuid))
	return err
}

// Originate places an outbound call via FreeSWITCH with a pre-generated callUUID.
// The UUID is set via origination_uuid so the caller knows it before FS responds.
// This is the primary call ID used throughout the system (API, Kafka, relay).
func (es *EventSocket) Originate(callUUID, target, callerID, callBotId, scenario string) error {
	var endpoint string
	if strings.Contains(target, "/") {
		endpoint = target
	} else {
		endpoint = fmt.Sprintf("loopback/%s/%s", target, es.config.Domain)
	}

	cmd := fmt.Sprintf(
		"originate {origination_uuid=%s,my_call_id=%s,callbot_bridge=true,origination_caller_id_number=%s,ignore_early_media=true,absolute_codec_string=PCMU@8000h@20i}%s &park",
		callUUID,
		callUUID,
		callerID,
		endpoint,
	)

	result, err := es.SendOriginate(cmd)
	if err != nil {
		return fmt.Errorf("originate %s: %w", target, err)
	}

	result = strings.TrimSpace(result)
	if strings.HasPrefix(result, "+OK") {
		return nil
	}

	return fmt.Errorf("originate failed: %s", result)
}
