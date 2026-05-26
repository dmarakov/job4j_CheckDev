package ru.checkdev.notification.telegram.service;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.checkdev.notification.domain.Profile;
import ru.checkdev.notification.util.CircuitBreaker;
import ru.checkdev.notification.util.Retry;

/**
 * Класс реализует методы get и post для отправки сообщений через WebClient
 *
 * @author Dmitry Stepanov, user Dmitry
 * @since 12.09.2023
 */
@org.springframework.context.annotation.Profile("default")
@Service
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class TgAuthCallWebClient implements TgCall {
    @Value("${server.auth}")
    private String urlServiceAuth;

    private final Retry retry = new Retry(3, 1000);
    private final CircuitBreaker circuitBreaker = new CircuitBreaker(2);

    /**
     * Метод get
     *
     * @param url URL http
     * @return Mono<Person>
     */
    @Override
    public Mono<Profile> doGet(String url) {
        return Mono.fromCallable(
                () -> retry.exec(
                    () -> circuitBreaker.exec(
                        () -> WebClient.create(urlServiceAuth)
                            .get()
                            .uri(url)
                            .retrieve()
                            .bodyToMono(Profile.class)
                            .block(),
                        null),
                    null)
            )
            .subscribeOn(Schedulers.boundedElastic())
            .doOnError(err -> log.error("doGet failed: {}", err.getMessage()));
    }

    /**
     * Метод POST
     *
     * @param url     URL http
     * @param profile Body PersonDTO.class
     * @return Mono<Person>
     */
    @Override
    public Mono<Object> doPost(String url, Profile profile) {
        return Mono.fromCallable(
                () -> retry.exec(
                    () -> circuitBreaker.exec(
                        () -> WebClient.create(urlServiceAuth)
                            .post()
                            .uri(url)
                            .bodyValue(profile)
                            .retrieve()
                            .bodyToMono(Object.class)
                            .doOnError(err -> log.error("API not found: {}", err.getMessage()))
                            .block(),
                        null),
                    null)

            )
            .subscribeOn(Schedulers.boundedElastic())
            .doOnError(err -> log.error("doPost failed after retries: {}", err.getMessage()));
    }

    @Override
    public Mono<Object> doPost(String url) {
        return Mono.fromCallable(
                () -> retry.exec(
                    () -> circuitBreaker.exec(
                        () -> WebClient.create(urlServiceAuth)
                            .post()
                            .uri(url)
                            .retrieve()
                            .bodyToMono(Object.class)
                            .doOnError(err -> log.error("API not found: {}", err.getMessage()))
                            .block(),
                        null
                    ), null
                )
            )
            .subscribeOn(Schedulers.boundedElastic())
            .doOnError(err -> log.error("doPost failed after retries: {}", err.getMessage()));
    }
}
