package main

import (
	"encoding/json"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func parseTxnMessage(msg maelstrom.Message) ([]txnOp, error) {
	var body txnMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return nil, err
	}
	var ops []txnOp
	for _, op := range body.Txn {
		key := int(op[1].(float64))
		if op[0] == readOp {
			ops = append(ops, txnOp{rw: readOp, key: key, list: nil})
		} else if op[0] == writeOp {
			ops = append(ops, txnOp{rw: writeOp, key: key, list: []int{int(op[2].(float64))}})
		}
	}
	return ops, nil
}

func parseGossipMessage(msg maelstrom.Message) (string, writeTxn, error) {
	var body gossipMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return "", writeTxn{}, err
	}
	var elems []writeElement
	for _, e := range body.Writes {
		elems = append(elems, writeElement{
			key: int(e[0].(float64)),
			lv: listVal{
				clock:  int(e[1].(float64)),
				nodeId: int(e[2].(float64)),
				index:  int(e[3].(float64)),
				val:    int(e[4].(float64)),
			}})
	}
	return body.Id, writeTxn{clock: body.Clock, writes: elems}, nil
}

func parseGossipOkMessage(msg maelstrom.Message) (string, int, error) {
	var body gossipOkMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return "", 0, err
	}
	return body.Id, body.ReqClock, nil
}

func formatGossipMessageBody(id string, txn writeTxn) map[string]any {
	var formattedTxn [][]any
	for _, op := range txn.writes {
		formattedTxn = append(formattedTxn, []any{op.key, op.lv.clock, op.lv.nodeId, op.lv.index, op.lv.val})
	}
	return map[string]any{"type": "gossip", "id": id, "writes": formattedTxn}
}

func formatList(list []listVal) []int {
	var vals []int
	for _, elem := range list {
		vals = append(vals, elem.val)
	}
	return vals
}

func getNodeId(id string) int {
	stripped := id[1:]

	i, err := strconv.Atoi(stripped)
	if err != nil {
		panic("getNodeId: " + err.Error())
	}
	return i
}
