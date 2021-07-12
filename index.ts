import {
    Application,
    ConsoleLogger,
    IDispatchContext,
    IMessageDispatcher,
    IValidateResult,
    JsonMessageEncoder,
} from "@walmartlabs/cookie-cutter-core";

import got from 'got';
import { kafkaSource } from "@walmartlabs/cookie-cutter-kafka";

class Telemetry {
    push(msg: any): Promise<any>{
        return got.post('https://preprod.ntp.net.in/content/data/v1/telemetry', {
            json: msg,
            responseType: 'json'
        });
    }
}

const telemetry = new Telemetry();

class AnyDispatcher implements IMessageDispatcher {
    canDispatch(msg: any): boolean {
        if(msg) return true;
    }
    canHandleInvalid?: () => boolean;
    
    dispatch(msg: any, ctx: IDispatchContext<any>, metadata: { validation: IValidateResult; }): Promise<any> {
        ctx.logger.info(`got message ${msg}`);
        ctx.logger.info(metadata.validation.message);
        return telemetry.push(msg).then(response => console.log(response.body));
    }
}

Application.create()
    .input()
    .add(
        kafkaSource({
            broker: "homeserver:9094",
            topics: "telemetry",
            encoder: new JsonMessageEncoder(),
            group: "event-pusher",
        })
    )
    .done()
    .logger(new ConsoleLogger())
    .dispatch(new AnyDispatcher())
    .run();