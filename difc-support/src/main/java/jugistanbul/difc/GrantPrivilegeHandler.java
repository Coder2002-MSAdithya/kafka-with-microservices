package jugistanbul.difc;

import org.apache.kafka.common.message.PollPrivsReqResponseData;

@FunctionalInterface
public interface GrantPrivilegeHandler {

    void onPrivilegeRequest(PollPrivsReqResponseData pending);
}
