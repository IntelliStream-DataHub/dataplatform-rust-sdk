{# The package page has no content of its own: classes are grouped by hand in index.rst,
   so a class missing from those groups is an orphan page, and -W fails the build. #}
{% if obj.display and is_own_page %}
:orphan:

{{ obj.id }}
{{ "=" * obj.id|length }}

.. py:module:: {{ obj.name }}
{% endif %}
