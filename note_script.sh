

python3 test_scripts/sp_test_event_hub.py \
    --connection_str='Endpoint=sb://nvs-event-hub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lyG937GhPMtGPyJ5+UH/w9fGR88xZ/FHM+AEhBtJRvA=' \
    --event_hub_name='stock-hub' 




python3 test_scripts/old_key_script.py \
    --namespace="nvs-event-hub-namespace" \
    --event_hub_name="stock-hub" \
    --key_name='$ConnectionString' \
    --key_value='Endpoint=sb://nvs-event-hub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=lyG937GhPMtGPyJ5+UH/w9fGR88xZ/FHM+AEhBtJRvA='
