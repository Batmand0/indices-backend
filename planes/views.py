from rest_framework.generics import ListCreateAPIView, RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from backend.permissions import IsAdminUserOrReadOnly
from .serializers import PlanGETSerializer, PlanPOSTSerializer
from .models import Plan


class PlanList(ListCreateAPIView):
    queryset = Plan.objects.all()
    serializer_class = PlanGETSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

    def post(self, request, *args, **kwargs):
        # Si estamos creando (POST) entonces cambiamos a un serializer que no utilice depth = 1
        serializer = PlanPOSTSerializer(data = request.data)
        if serializer.is_valid():
            self.object = serializer.save()
            headers = self.get_success_headers(serializer.data)
            # Aqui serializamos el nuevo plan con depth = 1
            new_plan = PlanGETSerializer(self.object)
            return Response(new_plan.data, status = status.HTTP_201_CREATED, headers = headers)
        return Response(serializer.errors, status = status.HTTP_400_BAD_REQUEST)

class PlanDetail(RetrieveUpdateDestroyAPIView):
    queryset = Plan.objects.all()
    serializer_class = PlanGETSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        instance = self.get_object()
        # Si estamos actualizando (PUT o PATCH) entonces cambiamos a un serializer que no utilice depth = 1
        serializer = PlanPOSTSerializer(instance, data=request.data, partial=partial)
        if serializer.is_valid():
            self.perform_update(serializer)

            if getattr(instance, '_prefetched_objects_cache', None):
                # If 'prefetch_related' has been applied to a queryset, we need to
                # forcibly invalidate the prefetch cache on the instance.
                instance._prefetched_objects_cache = {}
            return Response(serializer.data)
        return Response(serializer.errors, status = status.HTTP_400_BAD_REQUEST)