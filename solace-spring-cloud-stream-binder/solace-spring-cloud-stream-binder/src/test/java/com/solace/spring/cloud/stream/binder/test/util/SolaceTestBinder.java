package com.solace.spring.cloud.stream.binder.test.util;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.AbstractPollableConsumerTestBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceTestBinder
		extends AbstractPollableConsumerTestBinder<SolaceMessageChannelBinder, ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>> {

	private final JCSMPSession jcsmpSession;
	private final SempV2Api sempV2Api;
	private final AnnotationConfigApplicationContext applicationContext;
	private final Set<String> queues = new HashSet<>();
	private final Map<String, String> bindingNameToQueueName = new HashMap<>();
	private final Map<String, String> bindingNameToErrorQueueName = new HashMap<>();
	private static final Log logger = LogFactory.getLog(SolaceTestBinder.class);

	public SolaceTestBinder(JCSMPSession jcsmpSession, SempV2Api sempV2Api) {
		this.applicationContext = new AnnotationConfigApplicationContext(Config.class);
		this.jcsmpSession = jcsmpSession;
		this.sempV2Api = sempV2Api;
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, new SolaceQueueProvisioner(jcsmpSession));
		binder.setApplicationContext(this.applicationContext);
		this.setPollableConsumerBinder(binder);
	}

	public AnnotationConfigApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel moduleInputChannel,
												ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		preBindCaptureConsumerResources(name, group, properties);
		Binding<MessageChannel> binding = super.bindConsumer(name, group, moduleInputChannel, properties);
		captureConsumerResources(binding, group, properties.getExtension());
		return binding;
	}

	@Override
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name, String group,
																		PollableSource<MessageHandler> inboundBindTarget,
																		ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		preBindCaptureConsumerResources(name, group, properties);
		Binding<PollableSource<MessageHandler>> binding = super.bindPollableConsumer(name, group, inboundBindTarget, properties);
		captureConsumerResources(binding, group, properties.getExtension());
		return binding;
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel,
												ExtendedProducerProperties<SolaceProducerProperties> properties) {
		if (properties.getRequiredGroups() != null) {
			Arrays.stream(properties.getRequiredGroups())
					.forEach(g -> preBindCaptureProducerResources(name, g, properties));
		}

		return super.bindProducer(name, moduleOutputChannel, properties);
	}

	public String getConsumerQueueName(Binding<?> binding) {
		return bindingNameToQueueName.get(binding.getBindingName());
	}

	public String getConsumerErrorQueueName(Binding<?> binding) {
		return bindingNameToErrorQueueName.get(binding.getBindingName());
	}

	private void preBindCaptureConsumerResources(String name, String group, ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
		if (SolaceProvisioningUtil.isAnonQueue(group)) return; // we don't know any anon resource names before binding

		SolaceProvisioningUtil.QueueNames queueNames = SolaceProvisioningUtil.getQueueNames(name, group, consumerProperties, false);

		// values set here may be overwritten after binding
		queues.add(queueNames.getConsumerGroupQueueName());
		if (consumerProperties.getExtension().isAutoBindErrorQueue()) {
			queues.add(queueNames.getErrorQueueName());
		}
	}

	private void captureConsumerResources(Binding<?> binding, String group, SolaceConsumerProperties consumerProperties) {
		String queueName = extractBindingDestination(binding);
		bindingNameToQueueName.put(binding.getBindingName(), queueName);
		if (!SolaceProvisioningUtil.isAnonQueue(group)) {
			queues.add(queueName);
		}
		if (consumerProperties.isAutoBindErrorQueue()) {
			String errorQueueName = StringUtils.hasText(consumerProperties.getErrorQueueNameOverride()) ?
					consumerProperties.getErrorQueueNameOverride() :
					extractErrorQueueName(binding);
			queues.add(errorQueueName);
			bindingNameToErrorQueueName.put(binding.getBindingName(), errorQueueName);
		}
	}

	private void preBindCaptureProducerResources(String name, String group, ExtendedProducerProperties<SolaceProducerProperties> producerProperties) {
		String queueName = SolaceProvisioningUtil.getQueueName(name, group, producerProperties);
		queues.add(queueName);
	}

	private String extractBindingDestination(Binding<?> binding) {
		String destination = (String) binding.getExtendedInfo().getOrDefault("bindingDestination", "");
		assertThat(destination).startsWith("SolaceConsumerDestination");
		Matcher matcher = Pattern.compile("queueName='(.*?)'").matcher(destination);
		assertThat(matcher.find()).isTrue();
		return matcher.group(1);
	}

	private String extractErrorQueueName(Binding<?> binding) {
		String destination = (String) binding.getExtendedInfo().getOrDefault("bindingDestination", "");
		assertThat(destination).startsWith("SolaceConsumerDestination");
		Matcher matcher = Pattern.compile("errorQueueName='(.*?)'").matcher(destination);
		assertThat(matcher.find()).isTrue();
		return matcher.group(1);
	}

	@Override
	public void cleanup() {
		for (String queueName : queues) {
			try {
				logger.info(String.format("De-provisioning queue %s", queueName));
				Queue queue;
				try {
					queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				} catch (Exception e) {
					//This is possible as we eagerly add queues to cleanup in preBindCaptureConsumerResources()
					logger.info(String.format("Skipping de-provisioning as queue name is invalid; queue was never provisioned", queueName));
					continue;
				}
				jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			} catch (JCSMPException e) {
				try {
					sempV2Api.config().deleteMsgVpnQueue(
							(String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName);
				} catch (ApiException e1) {
					RuntimeException toThrow = new RuntimeException(e);
					toThrow.addSuppressed(e1);
					throw toThrow;
				}
			}
		}
	}

	@Configuration
	@EnableIntegration
	static class Config {}
}
