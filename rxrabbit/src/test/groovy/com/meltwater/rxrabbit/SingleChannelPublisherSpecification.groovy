package com.meltwater.rxrabbit

import com.meltwater.rxrabbit.impl.SingleChannelPublisher
import com.meltwater.rxrabbit.util.ConstantBackoffAlgorithm
import com.rabbitmq.client.AMQP
import rx.schedulers.Schedulers
import spock.lang.Specification

class SingleChannelPublisherSpecification extends Specification {

    def channelFactory = Mock(ChannelFactory)
    def scheduler = Schedulers.io()
    def metrics = Stub(PublishEventListener)
    def channel = Mock(PublishChannel)

    def 'if broker does not confirms within timeout re-tries until error'(){
        setup:
            SingleChannelPublisher publisher = new SingleChannelPublisher(channelFactory, true, 3, scheduler, metrics, 1, 1, 1, new ConstantBackoffAlgorithm(100))

        when:'publishing'
            boolean errorReported = publisher.call(ex(), rk('key'), new AMQP.BasicProperties(), pl("".getBytes()))
                    .toObservable()
                    .take(1)
                    .map {false}
                    .onErrorReturn {true}
                    .toBlocking()
                    .last()

        then:'a channel is created and basicPublish is called'
            1 * channelFactory.createPublishChannel() >> channel
            1 * channel.confirmSelect()
            1 * channel.addConfirmListener(_) //NOTE we don't care about the confirm listener as it will never be called
            3 * channel.getNextPublishSeqNo() >> 1l
            3 * channel.basicPublish(_, _, _ ,_)
            0 * _
        and:
            assert errorReported
    }

    def 'returns after successful publish if publisher confirm is false'(){
        setup:
            SingleChannelPublisher publisher = new SingleChannelPublisher(channelFactory, false, 3, scheduler, metrics, 1, 1, 1, new ConstantBackoffAlgorithm(100))

        when:'publishing'
            boolean errorReported = publisher.call(ex(), rk('key'), new AMQP.BasicProperties(), pl("".getBytes()))
                    .toObservable()
                    .take(1)
                    .map {false}
                    .onErrorReturn {true}
                    .toBlocking()
                    .last()

        then:'a channel is created and basicPublish is called'
            1 * channelFactory.createPublishChannel() >> channel
            1 * channel.getNextPublishSeqNo() >> 1l
            1 * channel.basicPublish(_, _, _ ,_)
            0 * _
        and:
            assert !errorReported

    }

    private Void triggerstuff(lRef) {

            lRef.get().handleAck(1l, false);
            lRef.get().handleAck(2l, false);
            lRef.get().handleAck(3l, false);

        return null
    }

    Payload pl(byte[] bytes) {
        new Payload(bytes)
    }

    RoutingKey rk(String key) {
        new RoutingKey(key)
    }

    Exchange ex() {
        new Exchange('exchange')
    }
}
