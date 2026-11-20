import json
import os
import sys

with open('agent.pid', 'w') as pid_file:
    pid_file.write(str(os.getpid()))

for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    params = request.get('params', {})
    if method == 'initialize':
        assert params['protocolVersion'] == 1
        caps = params.get('clientCapabilities', {})
        assert not caps.get('fs', {}).get('readTextFile', False)
        assert not caps.get('fs', {}).get('writeTextFile', False)
        assert not caps.get('terminal', False)
        assert not caps.get('terminalAuth', False)
        result = {'protocolVersion': 1, 'agentCapabilities': {'loadSession': True, 'sessionCapabilities': {'list': {}}}}
    elif method == 'session/new':
        assert os.path.normcase(os.path.normpath(params['cwd'])) == os.path.normcase(os.getcwd())
        assert params['mcpServers'] == []
        result = {'sessionId': 'session-1'}
    elif method == 'session/list':
        result = {'sessions': [{'sessionId': 'session-1', 'cwd': os.getcwd()}]}
        if 'cursor' not in params:
            result['nextCursor'] = 'page-2'
        else:
            assert params['cursor'] == 'page-2'
    elif method == 'session/load':
        assert os.path.normcase(os.path.normpath(params['cwd'])) == os.path.normcase(os.getcwd())
        assert params['mcpServers'] == []
        if params['sessionId'] == 'deleted':
            print(json.dumps({'jsonrpc': '2.0', 'id': request['id'], 'error': {'code': -32002, 'message': 'Session not found'}}), flush=True)
            continue
        print(json.dumps({'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': params['sessionId'], 'update': {'sessionUpdate': 'user_message_chunk', 'content': {'type': 'text', 'text': 'replay'}}}}), flush=True)
        print(json.dumps({'jsonrpc': '2.0', 'id': request['id'], 'result': {}}), flush=True)
        print(json.dumps({'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': params['sessionId'], 'update': {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'live'}}}}), flush=True)
        continue
    elif method == 'session/prompt':
        prompt_id = request['id']
        session = params['sessionId']
        cancel_prompt = params['prompt'][0]['text'] == 'cancel'
        close_prompt = params['prompt'][0]['text'] == 'close'
        cancelled = False
        updates = [
            {'sessionUpdate': 'agent_message_chunk', 'content': {'type': 'text', 'text': 'answer'}},
            {'sessionUpdate': 'agent_thought_chunk', 'content': {'type': 'text', 'text': 'thinking'}},
            {'sessionUpdate': 'tool_call', 'toolCallId': 'tool-1', 'title': 'run', 'status': 'in_progress'},
            {'sessionUpdate': 'tool_call_update', 'toolCallId': 'tool-1', 'status': 'completed'},
            {'sessionUpdate': 'plan', 'entries': []},
        ]
        if cancel_prompt:
            updates = [update for update in updates if update['sessionUpdate'] != 'tool_call_update']
        for update in updates:
            print(json.dumps({'jsonrpc': '2.0', 'method': 'session/update', 'params': {'sessionId': session, 'update': update}}), flush=True)
        print(json.dumps({'jsonrpc': '2.0', 'id': 100, 'method': 'session/request_permission', 'params': {'sessionId': session, 'toolCall': {'toolCallId': 'tool-1'}, 'options': [{'optionId': 'choice-2', 'name': 'Reject', 'kind': 'reject_once'}, {'optionId': 'choice-1', 'name': 'Allow', 'kind': 'allow_once'}]}}), flush=True)
        continue
    elif method == 'session/cancel':
        assert params['sessionId'] == session
        cancelled = True
        continue
    elif method is None and request.get('id') == 100:
        outcome = request['result']['outcome']
        if cancel_prompt or close_prompt:
            assert (cancelled or close_prompt) and outcome == {'outcome': 'cancelled'}
            if close_prompt:
                with open('permission-cancelled', 'w') as marker:
                    marker.write('cancelled')
            reason = 'cancelled'
        else:
            assert outcome == {'outcome': 'selected', 'optionId': 'choice-2'}
            reason = 'end_turn'
        print(json.dumps({'jsonrpc': '2.0', 'id': prompt_id, 'result': {'stopReason': reason}}), flush=True)
        continue
    else:
        continue
    print(json.dumps({'jsonrpc': '2.0', 'id': request['id'], 'result': result}), flush=True)
