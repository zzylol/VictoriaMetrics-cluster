ARG base_image=non-existing
FROM $base_image

EXPOSE 8482
EXPOSE 8400
EXPOSE 8401
EXPOSE 8410

ENTRYPOINT ["/vmsketch-prod"]
ARG src_binary=non-existing
COPY $src_binary ./vmsketch-prod