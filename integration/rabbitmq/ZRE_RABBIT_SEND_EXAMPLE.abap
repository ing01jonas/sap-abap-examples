REPORT ZRE_RABBIT_SEND_EXAMPLE.
*&---------------------------------------------------------------------*
*& Report ZRE_RABBIT_SEND_EXAMPLE
*&
*&---------------------------------------------------------------------*
*& Elaboró Jonathan Morales
*& Envió de xml y json de materiales a rabbitmq
*& Se necesita crear un RFC destination de tipo G que apunte al host y puerto
*& de la api de rabbit.
*&---------------------------------------------------------------------*


CLASS lcl_rabbit_send DEFINITION.
  PUBLIC SECTION.
    TYPES :
      BEGIN OF ty_s_mard,
        werks TYPE mard-werks,
        lgort TYPE mard-lgort,
        lfgja TYPE mard-lfgja,
        lfmon TYPE mard-lfmon,
        labst TYPE mard-labst,
      END OF ty_s_mard,
      BEGIN OF ty_data_str,
        matnr TYPE matnr,
        mtart TYPE mtart,
        meins TYPE meins,
        ean11 TYPE ean11,
        mard  TYPE STANDARD TABLE OF ty_s_mard WITH DEFAULT KEY,
      END OF ty_data_str,
      BEGIN OF ty_msg_properties,
        content_type TYPE string,
      END OF ty_msg_properties,
      BEGIN OF ty_rabbit_publish_str,
        properties       TYPE ty_msg_properties,
        routing_key      TYPE string,
        payload          TYPE string,
        payload_encoding TYPE string,
      END OF ty_rabbit_publish_str.

    CLASS-METHODS : main
      IMPORTING
        iv_mat   TYPE matnr
        iv_vhost TYPE char20
        iv_excha TYPE char200
        iv_rkey  TYPE char200
        iv_dest  TYPE rfcdest
      ,

      get_material_data
        IMPORTING iv_matnr       TYPE matnr
        RETURNING VALUE(rt_data) TYPE ty_data_str,
      create_json_string
        IMPORTING
          is_data TYPE ty_data_str
        EXPORTING
          ev_json TYPE string.
  PRIVATE SECTION.

    CLASS-METHODS create_xml_string
      IMPORTING
        is_data TYPE lcl_rabbit_send=>ty_data_str
      EXPORTING
        ev_xml  TYPE string.
    CLASS-METHODS create_rabbit_json_payload
      IMPORTING
        iv_routing_key      TYPE string
        iv_payload          TYPE string
        iv_payload_encoding TYPE string DEFAULT 'string'
        iv_content_type     TYPE string  DEFAULT 'text/plain'
      EXPORTING
        ev_payload          TYPE string.
    CLASS-METHODS send_request
      IMPORTING
        iv_virtualhost TYPE char20
        iv_exchange    TYPE char200
        iv_payload     TYPE string
        io_client      TYPE REF TO if_http_client
      EXPORTING
        ev_code        TYPE string
        ev_reason      TYPE string
        ev_response    TYPE string.
ENDCLASS.

CLASS lcl_rabbit_send IMPLEMENTATION.
  METHOD main.

    DATA(ls_data) = get_material_data( iv_mat ).

    create_json_string(
      EXPORTING
        is_data         = ls_data
      IMPORTING
        ev_json         = DATA(lv_json_request)
    ).

    create_xml_string(
        EXPORTING
            is_data = ls_data
        IMPORTING
            ev_xml = DATA(lv_xml_request)
    ).

    create_rabbit_json_payload(
        EXPORTING
            iv_routing_key = CONV #( iv_rkey )
            iv_payload = lv_json_request
            iv_content_type = 'application/json'
         IMPORTING
            ev_payload = DATA(lv_rabbitjs_payload)
    ).

    create_rabbit_json_payload(
        EXPORTING
            iv_routing_key = CONV #( iv_rkey )
            iv_payload = lv_xml_request
            iv_content_type = 'application/xml'
         IMPORTING
            ev_payload = DATA(lv_rabbitxml_payload)
    ).

    DATA : lo_client TYPE REF TO if_http_client.
    " Send payload to rabbit
    cl_http_client=>create_by_destination(
          EXPORTING
            destination              = iv_dest
          IMPORTING
            client                   = lo_client
          EXCEPTIONS
            argument_not_found       = 1
            destination_not_found    = 2
            destination_no_authority = 3
            plugin_not_active        = 4
            internal_error           = 5
            OTHERS                   = 6
        ).
    send_request(
        EXPORTING
          iv_virtualhost = iv_vhost
          iv_exchange = iv_excha
          iv_payload = lv_rabbitjs_payload
          io_client  = lo_client
        IMPORTING
          ev_code = DATA(lv_json_code)
          ev_reason = DATA(lv_json_reason)
          ev_response = DATA(lv_json_response)
    ).

    send_request(
        EXPORTING
          iv_virtualhost = iv_vhost
          iv_exchange = iv_excha
          iv_payload = lv_rabbitxml_payload
          io_client  = lo_client
        IMPORTING
          ev_code = DATA(lv_xml_code)
          ev_reason = DATA(lv_xml_reason)
          ev_response = DATA(lv_xml_response)
    ).

    lo_client->close(  ).

    cl_demo_output=>write_data( lv_json_request ).
    cl_demo_output=>write_data( lv_json_code ).
    cl_demo_output=>write_data( lv_json_response ).

    cl_demo_output=>write_data( lv_xml_request ).
    cl_demo_output=>write_data( lv_xml_code ).
    cl_demo_output=>write_data( lv_xml_response ).
    cl_demo_output=>display(  ).

  ENDMETHOD.
  METHOD get_material_data.
    DATA(lv_matnr) = CONV matnr( |{  iv_matnr ALPHA = IN }| ).
    SELECT SINGLE matnr,
          mtart,
          meins,
          ean11
      FROM mara
       INTO @DATA(ls_mara_data)
       WHERE matnr = @lv_matnr.

    SELECT werks,
           lgort,
           lfgja,
           lfmon,
           labst
           FROM mard
           INTO TABLE @DATA(lt_mard_data)
           WHERE matnr = @ls_mara_data-matnr.

    ls_mara_data-matnr = |{ ls_mara_data-matnr ALPHA = OUT }|.

    CALL FUNCTION 'CONVERSION_EXIT_CUNIT_OUTPUT'
      EXPORTING
        input          = ls_mara_data-meins
*       language       = SY-LANGU
      IMPORTING
*       long_text      =
        output         = ls_mara_data-meins
*       short_text     =
      EXCEPTIONS
        unit_not_found = 1
        OTHERS         = 2.

    IF sy-subrc <> 0.
      ls_mara_data-meins = 'NA'.
    ENDIF.
    rt_data = CORRESPONDING #( ls_mara_data ).
    rt_data-mard = CORRESPONDING #( lt_mard_data ).

  ENDMETHOD.

  METHOD create_json_string.

    ev_json = /ui2/cl_json=>serialize(
       data = is_data
       compress = abap_false
       pretty_name = /ui2/cl_json=>pretty_mode-camel_case
   ).

  ENDMETHOD.


  METHOD create_xml_string.
    " Example from https://blogs.sap.com/2021/07/13/create-an-xml-file-with-node-and-attribute-value/
    DATA(lo_xml_head_doc) = NEW cl_xml_document(  ).


    lo_xml_head_doc->create_with_data(
      EXPORTING
        dataobject = is_data
    ).

    lo_xml_head_doc->render_2_string(
        IMPORTING
            stream = ev_xml
     ).
  ENDMETHOD.


  METHOD create_rabbit_json_payload.

    DATA ls_payload TYPE ty_rabbit_publish_str.
    ls_payload-routing_key = iv_routing_key.
    ls_payload-payload = iv_payload.
    ls_payload-payload_encoding = iv_payload_encoding.
    ls_payload-properties-content_type = iv_content_type.

    ev_payload = /ui2/cl_json=>serialize(
       data = ls_payload
       compress = abap_true
       pretty_name = /ui2/cl_json=>pretty_mode-low_case
   ).
  ENDMETHOD.


  METHOD send_request.
    DATA(lv_uri) = |/api/exchanges/{ iv_virtualhost }/{ iv_exchange }/publish|.

    cl_http_utility=>set_request_uri(
      EXPORTING
        request    = io_client->request
        uri        = lv_uri
*        multivalue = 0
    ).
    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid TYPE 'E' NUMBER sy-msgno
        WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ENDIF.

    io_client->propertytype_accept_cookie = if_http_client=>co_enabled.
    io_client->request->set_content_type(  content_type = 'application/json; charset=utf-8' ).
    io_client->request->set_method( if_http_request=>co_request_method_post ).

    io_client->request->set_cdata(
      EXPORTING
        data   = iv_payload
*        offset = 0
*        length = -1
    ).

    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid TYPE 'E' NUMBER sy-msgno
        WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ENDIF.
    io_client->send(
*      EXPORTING
*        timeout                    = co_timeout_default
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3
        http_invalid_timeout       = 4
        OTHERS                     = 5
    ).

    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid TYPE 'E' NUMBER sy-msgno
        WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ENDIF.

    io_client->receive(
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3
        OTHERS                     = 4
    ).

    ev_code = io_client->response->get_header_field( '~status_code' ).
    ev_reason = io_client->response->get_header_field( '~status_reason' ).

    ev_response = io_client->response->get_cdata( ).
  ENDMETHOD.

ENDCLASS.
PARAMETERS : p_mat   TYPE mara-matnr OBLIGATORY,
             p_vhost TYPE c LENGTH 20 VISIBLE LENGTH 10 DEFAULT '%2f' LOWER CASE OBLIGATORY,
             p_rkey  TYPE c LENGTH 200 VISIBLE LENGTH 30 DEFAULT 'test_routing' LOWER CASE OBLIGATORY,
             p_exch  TYPE c LENGTH 200 VISIBLE LENGTH 30 DEFAULT 'amq.direct' LOWER CASE OBLIGATORY,
             p_des   TYPE rfcdest DEFAULT 'RABBITMQ' OBLIGATORY.

START-OF-SELECTION.
  lcl_rabbit_send=>main(
    iv_mat = p_mat
    iv_vhost = p_vhost
    iv_excha = p_exch
    iv_rkey = p_rkey
    iv_dest = p_des
  ).
