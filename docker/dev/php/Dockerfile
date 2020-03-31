FROM php:7.3-cli-alpine3.10

# https://getcomposer.org/doc/03-cli.md#composer-allow-superuser
ENV COMPOSER_ALLOW_SUPERUSER 1
ARG HOST_USER_ID

# PHP: Copy configuration files & remove dist files
RUN mkdir /phpIni
COPY dev/php/files/bin/ /usr/local/bin/

# SYS: Install required packages
RUN apk --no-cache upgrade && \
    apk --no-cache add bash git sudo openssh autoconf gcc g++ make gettext shadow

RUN apk add librdkafka librdkafka-dev \
        --update-cache --repository http://dl-3.alpinelinux.org/alpine/edge/community

# Set host user id if given, to avoid permission conflicts in dev
RUN /bin/bash -c 'if [ -n "$HOST_USER_ID" ] && [ "$HOST_USER_ID" -lt 60000 ]; then \
        usermod -u ${HOST_USER_ID} www-data; \
    fi'

# PHP: Install php extensions
RUN pecl channel-update pecl.php.net && \
    pecl install rdkafka pcov && \
    docker-php-ext-install pcntl && \
    php-ext-enable rdkafka pcntl pcov

# COMPOSER: install binary
RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/bin --filename=composer

USER 'www-data'

# COMPOSER: install dependencies
RUN composer global require hirak/prestissimo

WORKDIR /var/www/html

