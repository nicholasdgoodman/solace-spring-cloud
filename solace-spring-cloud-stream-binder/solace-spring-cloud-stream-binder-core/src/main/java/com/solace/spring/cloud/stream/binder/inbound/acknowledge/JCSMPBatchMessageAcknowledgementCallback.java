package com.solace.spring.cloud.stream.binder.inbound.acknowledge;

import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import com.solace.spring.cloud.stream.binder.util.RetryableTaskService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.lang.Nullable;

public class JCSMPBatchMessageAcknowledgementCallback implements AcknowledgmentCallback {
  private JCSMPAcknowledgementCallback acknowledgementCallback;
  private Status status = Status.ACCEPT;

  private static final Log logger = LogFactory.getLog(JCSMPBatchMessageAcknowledgementCallback.class);

	public JCSMPBatchMessageAcknowledgementCallback(MessageContainer messageContainer, FlowReceiverContainer flowReceiverContainer,
								 boolean hasTemporaryQueue,
								 RetryableTaskService taskService,
								 @Nullable ErrorQueueInfrastructure errorQueueInfrastructure) {

    this.acknowledgementCallback = new JCSMPAcknowledgementCallback(
      messageContainer, flowReceiverContainer, hasTemporaryQueue, taskService, errorQueueInfrastructure);
  }

	@Override
	public void acknowledge(Status status) {
		this.status = status;

    if(logger.isTraceEnabled()) {
      logger.trace(String.format("acknowledge single message in batch, status %s", status));
    }
	}

  public void acknowledgeInnerCallback(Status batchStatus) {
    logger.trace(String.format("message status %s, batchStatus %s", status, batchStatus));
    if(status == Status.REJECT || batchStatus == Status.REJECT) {
      acknowledgementCallback.acknowledge(Status.REJECT);
    } else if(status == Status.REQUEUE || batchStatus == Status.REQUEUE) {
      acknowledgementCallback.acknowledge(Status.REQUEUE);
    } else {
      acknowledgementCallback.acknowledge(Status.ACCEPT);
    }
  }
  
  public JCSMPAcknowledgementCallback getInnerCallback() {
    return acknowledgementCallback;
  }
}
