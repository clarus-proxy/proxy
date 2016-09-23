package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public interface PgsqlCommandResultMessage<T> extends PgsqlQueryResponseMessage {
    interface Details<T> {
        T get();

        default boolean isDedicatedTo(Class<? extends PgsqlCommandResultMessage<?>> msgType) {
            Type type1 = ((ParameterizedType) msgType.getGenericInterfaces()[0]).getActualTypeArguments()[0];
            Type type2 = ((ParameterizedType) this.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
            return type1.equals(type2);
        }

        @SuppressWarnings("unchecked")
        default <U> Details<U> cast() {
            return (Details<U>) this;
        }
    }
    boolean isSuccess();
    Details<T> getDetails();
    void setDetails(Details<T> details);
}
