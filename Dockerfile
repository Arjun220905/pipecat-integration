FROM ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 AS uv
FROM cgr.dev/chainguard/wolfi-base:latest@sha256:94fa13e26d449f7d78dbcd02d12ea37a0da80d24fcc3e1f17b95d515dec607a7 AS base
RUN apk add --no-cache python-3.13=3.13.18_git20261030-r0 libstdc++=16.2.0-r3 libgomp=16.2.0-r3
WORKDIR /app

# The Pipecat-pinned soundfile wheel bundles vulnerable libsndfile 1.2.2.
# Build the upstream CVE-2026-37555 fix instead; keep provenance in the image.
FROM base AS sndfile
RUN apk add --no-cache build-base cmake samurai
ADD --checksum=sha256:8bb7392c985035b0fb9c9df73535cdb2534a675f9b920598f93d3285c3eef7f9 \
    https://codeload.github.com/libsndfile/libsndfile/tar.gz/c32c090ff9bb45e1dae637957c9c9b47b3967623 /tmp/sndfile.tar.gz
RUN mkdir /source && tar -xzf /tmp/sndfile.tar.gz -C /source --strip-components=1 \
    && cmake -S /source -B /build -G Ninja -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=OFF \
       -DCMAKE_INSTALL_LIBDIR=lib -DBUILD_PROGRAMS=OFF -DBUILD_EXAMPLES=OFF -DENABLE_EXTERNAL_LIBS=OFF -DENABLE_MPEG=OFF \
    && cmake --build /build && DESTDIR=/native cmake --install /build \
    && mkdir -p /native/usr/local/share/libsndfile \
    && cp /source/COPYING /native/usr/local/share/libsndfile/ \
    && printf '%s\n' 'https://github.com/libsndfile/libsndfile/commit/c32c090ff9bb45e1dae637957c9c9b47b3967623' \
       > /native/usr/local/share/libsndfile/SOURCE

FROM base AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-binary-package soundfile --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-binary-package soundfile --no-editable

FROM base AS runtime
RUN addgroup -g 10001 bridge && adduser -D -H -u 10001 -G bridge bridge \
    && mkdir /state && chown bridge:bridge /state && chmod 700 /state \
    && find / -xdev -type f -perm /6000 -exec chmod a-s {} +
COPY --from=build /app/.venv /app/.venv
COPY --from=sndfile /native/usr/local/lib/libsndfile.so* /usr/local/lib/
COPY --from=sndfile /native/usr/local/share/libsndfile /usr/local/share/libsndfile
ENV LD_LIBRARY_PATH=/usr/local/lib PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    MEETSTREAM_STATE_DIR=/state HOME=/tmp HOST=0.0.0.0 PORT=8000 CONTAINERIZED=1 \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 NUMBA_NUM_THREADS=1
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/app/.venv/bin/python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2)"]
ENTRYPOINT ["/app/.venv/bin/meetstream-pipecat"]
CMD ["serve"]
