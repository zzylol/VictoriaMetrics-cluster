ARG base_image=non-existing
FROM $base_image

EXPOSE 8582
EXPOSE 8500
EXPOSE 8501

ENTRYPOINT ["/vmsketch-prod"]
ARG src_binary=non-existing
COPY $src_binary ./vmsketch-prod